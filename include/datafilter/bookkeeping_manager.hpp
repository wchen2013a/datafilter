#ifndef DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_
#define DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_

#include <algorithm>
#include <atomic>
#include <cctype> // std::tolower
#include <condition_variable>
#include <execution>
#include <fstream>
#include <mutex>
#include <queue>
#include <string>
#include <utility>

#include "datafilter/datafilter_structs.hpp"
#include "iomanager/IOManager.hpp"

using namespace dunedaq::iomanager;

namespace dunedaq {
namespace datafilter {

struct RunInfo {
  std::atomic<unsigned int> run_number{0};
  std::atomic<unsigned int> file_index{0};

  mutable std::mutex mutex; // mutable allows const methods to lock

  // Set the run information
  void set(unsigned int run, unsigned int file_idx) {
    std::lock_guard<std::mutex> lock(mutex);
    run_number = run;
    file_index = file_idx;
  }

  // Get the run information
  std::pair<unsigned int, unsigned int> get() const {
    std::lock_guard<std::mutex> lock(mutex);
    return {run_number.load(), file_index.load()};
  }
};

struct BookkeepingReceiver {
  RunInfo &run_info;

  // Thread control
  std::atomic<bool> stop_flag{false};
  std::unique_ptr<std::thread> receiver_thread;
  std::unique_ptr<std::thread> writer_thread;
  std::mutex queue_mutex;
  std::condition_variable queue_cv;
  std::queue<nlohmann::json> bk_queue;
  std::atomic<unsigned int> received_cnt{0};

  // Transfer rate tracking.
  std::atomic<double> transfer_rate_mbps{0};
  std::mutex rate_mutex;

  std::string m_bk_rx_uid;  // DF listens here (bookkeeping0)
  std::string m_bk_tx_uid;  // DF forwards initial BK to FRW (bookkeeping1)
  std::string m_bk_trd_uid; // DF forwards completion to TRD (bookkeeping2)

  std::string m_session_name;
  // Datafilter ID
  std::string datafilter_id;
  mutable std::mutex id_mutex;

  std::string bk_file;
  std::mutex file_mutex;
  std::atomic<bool> have_file{false};

  std::atomic<bool> callback_registered{false};
  std::atomic<bool> first_bk_seen{false};

  // Completion tracking: set when the final TRD BK (kReRecorded or
  // kWriteFailed) is received. stop() waits on this before terminating.
  std::atomic<bool> all_bk_received{false};
  std::mutex all_bk_mutex;
  std::condition_variable all_bk_cv;

  std::atomic<double> transfer_rate_in_mbps{0.0};  // TD -> DF
  std::atomic<double> transfer_rate_out_mbps{0.0}; // DF -> Writer

  std::shared_ptr<
      dunedaq::iomanager::SenderConcept<dunedaq::datafilter::BookKeeping>>
      m_bk_sender;

  explicit BookkeepingReceiver(RunInfo &info, std::string id = "",
                               std::string bk_rx_uid = "",
                               std::string bk_tx_uid = "",
                               std::string session_name = "",
                               std::string bk_trd_uid = "")
      : run_info(info), datafilter_id(std::move(id)),
        m_bk_rx_uid(std::move(bk_rx_uid)), m_bk_tx_uid(std::move(bk_tx_uid)),
        m_session_name(std::move(session_name)),
        m_bk_trd_uid(std::move(bk_trd_uid)) {
    TLOG() << "BookkeepingReceiver initialized";
  }

  ~BookkeepingReceiver() {
    try {
      stop();
    } catch (...) {
    }
    TLOG() << "BookkeepingReceiver destroyed";
  }

  void start() {
    std::lock_guard<std::mutex> lock(queue_mutex);
    if (receiver_thread) {
      TLOG() << "Receiver already running";
      return;
    }

    stop_flag.store(false);
    writer_thread = std::make_unique<std::thread>([this]() {
      TLOG() << "Starting writer thread (ID: " << std::this_thread::get_id()
             << ")";
      this->write_to_file();
    });

    receiver_thread = std::make_unique<std::thread>([this]() {
      TLOG() << "Starting receiver thread (ID: " << std::this_thread::get_id()
             << ")";
      this->receive_bk();
      TLOG() << "Receiver thread exiting";
    });
    TLOG() << "Bookkeeping receiver started";
  }

  void stop() {
    TLOG() << "Initiating bookkeeping receiver shutdown";

    // Wait for the final TRD BookKeeping (kReRecorded or kWriteFailed)
    // before killing threads. This ensures all 3 expected BK entries
    // (TRD assigned_to_filter, FRW file_completed, TRD
    // re_recorded/write_failed) are written even if stop() is called before
    // FRW's write-wait completes. 10-minute timeout covers any realistic
    // FRW processing time.
    {
      std::unique_lock<std::mutex> lk(all_bk_mutex);
      bool got_final = all_bk_cv.wait_for(lk, std::chrono::minutes(10), [this] {
        return all_bk_received.load(std::memory_order_acquire);
      });
      if (!got_final)
        TLOG() << "stop(): timed out waiting for final TRD BK — "
                  "stopping anyway";
      else
        TLOG() << "stop(): final TRD BK confirmed, draining queue";
    }

    stop_flag.store(true);
    queue_cv.notify_all();

    if (receiver_thread && receiver_thread->joinable()) {
      receiver_thread->join();
    }
    receiver_thread.reset();

    if (writer_thread && writer_thread->joinable()) {
      writer_thread->join();
    }
    writer_thread.reset();

    TLOG() << "Bookkeeping receiver fully stopped";
  }

  void set_transfer_rate(double transfer_rate) {
    std::lock_guard<std::mutex> lock(rate_mutex);
    transfer_rate_mbps = transfer_rate;
  }

  double get_transfer_rate() {
    std::lock_guard<std::mutex> lock(rate_mutex);
    return transfer_rate_mbps.load();
  }

  void set_transfer_rate_in(double mbps) {
    transfer_rate_in_mbps.store(mbps, std::memory_order_relaxed);
  }

  void set_transfer_rate_out(double mbps) {
    transfer_rate_out_mbps.store(mbps, std::memory_order_relaxed);
  }
  double get_transfer_rate_in() const {
    return transfer_rate_in_mbps.load(std::memory_order_relaxed);
  }
  double get_transfer_rate_out() const {
    return transfer_rate_out_mbps.load(std::memory_order_relaxed);
  }

  std::string get_datafilter_id() const {
    std::lock_guard<std::mutex> lock(id_mutex);
    return datafilter_id;
  }

  static inline std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
      return static_cast<char>(std::tolower(c));
    });
    return s;
  }

private:
  // Forwards FRW's completion BK to TRD via bookkeeping2 (m_bk_trd_uid).
  // Translates kFileCompleted → kReRecorded so TRD's existing status check
  // passes. Preserves from_id="FilterResultWriter" so TRD's from_id check
  // passes unchanged.
  void send_completion_to_trd(const dunedaq::datafilter::BookKeeping &frw_bk) {
    if (m_bk_trd_uid.empty()) {
      TLOG() << "send_completion_to_trd: no TRD uid configured, skipping";
      return;
    }
    dunedaq::datafilter::BookKeeping fwd(m_bk_trd_uid);
    fwd.from_id = "FilterResultWriter";
    fwd.run_number = frw_bk.run_number;
    fwd.tr_status = (frw_bk.tr_status == to_string(TRStatus::kFileCompleted))
                        ? to_string(TRStatus::kReRecorded)
                        : to_string(TRStatus::kWriteFailed);
    fwd.tr_header_info = frw_bk.tr_header_info;

    auto sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(m_bk_trd_uid);
    if (!sender) {
      TLOG() << "send_completion_to_trd: failed to get sender for "
             << m_bk_trd_uid;
      return;
    }
    try {
      sender->send(std::move(fwd), std::chrono::milliseconds(2000));
      TLOG() << "BookkeepingReceiver: forwarded FRW completion ("
             << frw_bk.tr_status << " -> " << fwd.tr_status << ") to TRD via "
             << m_bk_trd_uid;
    } catch (const std::exception &e) {
      TLOG() << "send_completion_to_trd failed: " << e.what();
    }
  }

  void send_bk(dunedaq::datafilter::BookKeeping bk_info) {
    TLOG() << "Send bookkeeping info to FilterResultWriter";

    if (m_bk_tx_uid.empty()) {
      TLOG() << "send_bk: m_bk_tx_uid not configured — OKS topology "
                "error, skipping send";
      return;
    }
    m_bk_sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(m_bk_tx_uid);

    if (!m_bk_sender) {
      TLOG() << "Failed to get bookkeeping sender!";
      return;
    }

    try {
      m_bk_sender->send(std::move(bk_info), std::chrono::milliseconds(2000));
      TLOG() << "Successfully sent BookKeeping data.";

    } catch (const std::exception &e) {
      TLOG() << "Send failed (non-blocking, continuing): " << e.what();
    }
  }

  void receive_bk() {
    TLOG() << "Setting up bookkeeping receiver";

    if (m_bk_rx_uid.empty()) {
      TLOG() << "receive_bk: m_bk_rx_uid not configured — OKS topology "
                "error, skipping receiver setup";
      return;
    }
    TLOG() << "BK: attempting to bind receiver on uid=" << m_bk_rx_uid
           << " type=BookKeeping";

    auto cb_receiver =
        dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
            m_bk_rx_uid);
    if (!cb_receiver) {
      TLOG() << "Failed to get bookkeeping receiver";
      return;
    }

    // TLOG() << "Starting file writer thread";
    // std::thread file_writer([this, &bk_file]() {
    //   TLOG() << "File writer thread started (ID: " <<
    //   std::this_thread::get_id()
    //          << ")";
    //   this->write_to_file(bk_file, stop_flag);
    //   TLOG() << "File writer thread exiting";
    // });

    auto str_receiver_cb = [&](dunedaq::datafilter::BookKeeping bk) {
      if (stop_flag)
        return;

      const std::string from = to_lower(bk.from_id);

      const bool is_from_trdisp =
          (from.find("trdispatcher") != std::string::npos);
      const bool is_from_writer =
          (from.find("filterresultwriter") != std::string::npos);

      // Detect start of a new pipeline cycle (TRD assigns a new file).
      // Reset per-cycle counters so the cnt==1 gate and stop() wait work
      // correctly for every run, not just the first.
      if (is_from_trdisp &&
          bk.tr_status == to_string(TRStatus::kAssignedToFilter)) {
        received_cnt.store(0, std::memory_order_relaxed);
        all_bk_received.store(false, std::memory_order_release);
        TLOG() << "New pipeline cycle detected — resetting per-cycle "
                  "counters";
      }

      unsigned int cnt = ++received_cnt;

      TLOG() << "Processing bookkeeping # " << cnt << " from " << bk.from_id
             << " (Run: " << bk.run_number << ")";

      if (cnt == 1) {
        std::string file_index = "0";
        if (auto it = std::find_if(
                bk.file_attributes_info.begin(), bk.file_attributes_info.end(),
                [](const auto &p) { return p.first == "file_index"; });
            it != bk.file_attributes_info.end()) {
          file_index = it->second;
        }

        // send_bk() forwards initial BK to FRW if a BK output is
        // configured. Guard with try-catch so that a
        // missing/unconfigured output does not throw past this point
        // and skip run_info.set() and queuing below.
        if (!m_bk_tx_uid.empty()) {
          try {
            send_bk(bk);
          } catch (const std::exception &e) {
            TLOG() << "send_bk failed (non-critical, continuing): " << e.what();
          }
        }
        run_info.set(bk.run_number, std::stoi(file_index));
        TLOG() << "Set initial run info - Run: " << bk.run_number
               << " File Index: " << file_index;
      }

      auto in_mbps = get_transfer_rate_in();
      auto out_mbps = get_transfer_rate_out();
      // we only store in and out rate.
      auto transfer_rate = is_from_trdisp   ? in_mbps
                           : is_from_writer ? out_mbps
                                            : 0;

      TLOG() << "Transfer rate (ewma) " << transfer_rate << " Mbps";
      bk.transfer_rate = transfer_rate;

      auto datafilter_id = get_datafilter_id();
      bk.datafilter_id = datafilter_id;

      auto [run, file_idx] = run_info.get();
      {
        std::lock_guard<std::mutex> lock(file_mutex);
        bk_file = generate_bk_filename(run, file_idx);
        TLOG() << "Updated output file: " << bk_file;
        have_file.store(true, std::memory_order_release);
      }

      {
        std::lock_guard<std::mutex> lock(queue_mutex);
        bk_queue.push(to_json(bk));
        TLOG() << "Queued message (Queue size: " << bk_queue.size() << ")";
      }
      queue_cv.notify_one();

      // When FRW signals file completion (kFileCompleted or
      // kWriteFailed), forward a translated confirmation to TRD via
      // bookkeeping2. DataFilter is the intermediary — FRW no longer
      // sends directly to TRD.
      if (is_from_writer &&
          (bk.tr_status == to_string(TRStatus::kFileCompleted) ||
           bk.tr_status == to_string(TRStatus::kWriteFailed))) {
        send_completion_to_trd(bk);
      }

      // Detect TRD's final BK (kReRecorded or kWriteFailed) — sent after
      // TRD receives the forwarded completion and calls WriteJSON.
      // Signals stop() to unblock and drain cleanly.
      if (is_from_trdisp &&
          (bk.tr_status == to_string(TRStatus::kReRecorded) ||
           bk.tr_status == to_string(TRStatus::kWriteFailed))) {
        all_bk_received.store(true, std::memory_order_release);
        all_bk_cv.notify_all();
        TLOG() << "Final TRD BK received (status=" << bk.tr_status
               << ") — all bookkeeping complete.";
      }
    };

    cb_receiver->add_callback(str_receiver_cb);
    TLOG() << "Callback registered, entering main loop";

    while (!stop_flag.load()) {
      // Nothing to do here anymore; writer consumes queue.
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    TLOG() << "Cleaning up receiver";

    cb_receiver->remove_callback();
    {
      std::lock_guard<std::mutex> lk(queue_mutex);
    }
    queue_cv.notify_all(); // in case writer is waiting

    TLOG() << "Receiver cleanup complete";
  }

  nlohmann::json to_json(const BookKeeping &bk) {
    return nlohmann::json{{"entry_id", bk.entry_id},
                          {"conn_id", bk.conn_id},
                          {"from_id", bk.from_id},
                          {"datafilter_id", bk.datafilter_id},
                          {"node", bk.node},
                          {"tr_header_info", bk.tr_header_info},
                          {"file_attributes_info", bk.file_attributes_info},
                          {"tr_status", bk.tr_status},
                          {"file_send_list", bk.file_send_list},
                          {"file_send_status", bk.file_send_status},
                          {"transfer_rate", bk.transfer_rate}};
  }

  // Function to read existing transactions from the file
  nlohmann::json open_existing_bk(const std::string &filename) {
    std::ifstream file(filename);
    if (file.is_open()) {
      try {
        nlohmann::json existing_bk;
        file >> existing_bk;
        return existing_bk;
      } catch (const std::exception &e) {
        std::cerr << "Error reading JSON file: " << e.what() << std::endl;
      }
    }
    return nlohmann::json::array(); // Return an empty array if the file
                                    // doesn't exist or is invalid
  }

  void write_to_file() {
    TLOG() << "Setting up bookkeeping writer ";
    std::string current_file;
    nlohmann::json existing_bk = nlohmann::json::array();
    auto start_time = std::chrono::high_resolution_clock::now();
    int transaction_count = 0;

    for (;;) {
      std::unique_lock<std::mutex> lock(queue_mutex);
      if (!queue_cv.wait_for(lock, std::chrono::milliseconds(200), [&] {
            return !bk_queue.empty() || stop_flag.load();
          })) {
        if (stop_flag.load() && bk_queue.empty())
          break;
        continue;
      }
      if (bk_queue.empty() && stop_flag.load())
        break;

      // Drain-all
      std::vector<nlohmann::json> batch;
      batch.reserve(bk_queue.size());
      while (!bk_queue.empty()) {
        batch.emplace_back(std::move(bk_queue.front()));
        bk_queue.pop();
      }
      lock.unlock();

      // Get latest file name
      std::string out_file;
      {
        std::lock_guard<std::mutex> f(file_mutex);
        out_file = bk_file;
      }
      if (!have_file.load(std::memory_order_acquire)) {
        // No filename yet — return items to queue rather than losing
        // them.
        {
          std::lock_guard<std::mutex> rl(queue_mutex);
          for (auto &item : batch)
            bk_queue.push(std::move(item));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }

      // Rotate if needed
      if (out_file != current_file) {
        current_file = out_file;
        existing_bk = open_existing_bk(current_file);
        if (!existing_bk.is_array())
          existing_bk = nlohmann::json::array();
      }

      // Append batch
      for (auto &t : batch)
        existing_bk.push_back(std::move(t));

      // Flush
      std::ofstream file(current_file);
      if (file.is_open()) {
        file << existing_bk.dump(4);
      } else {
        std::cerr << "Failed to open file '" << current_file
                  << "' for writing!\n";
      }

      if (++transaction_count % 1 == 0) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                            end_time - start_time)
                            .count();
        std::cout << "Processed " << transaction_count << " transactions in "
                  << duration << " ms\n";
      }
    }
    TLOG() << "File writer thread exiting";
  }

  std::string generate_bk_filename(int run_number, int file_index) {
    std::ostringstream filename_oss;
    filename_oss << "bookkeeping_" << std::setw(6) << std::setfill('0')
                 << run_number << "_" << std::setw(4) << std::setfill('0')
                 << file_index << ".json";
    return filename_oss.str();
  }
};

} // namespace datafilter
} // namespace dunedaq

#endif // DATAFILTER_INCLUDE_BOOKKEEPINGRMANAGER_HPP_
