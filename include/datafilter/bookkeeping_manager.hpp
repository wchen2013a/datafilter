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

  std::string m_bk_rx_uid; // DF listens here
  std::string m_bk_tx_uid; // DF sends here (buffered)

  std::string m_session_name;
  // Datafilter ID
  std::string datafilter_id;
  mutable std::mutex id_mutex;

  std::string bk_file;
  std::mutex file_mutex;
  std::atomic<bool> have_file{false};

  std::atomic<bool> callback_registered{false};
  std::atomic<bool> first_bk_seen{false};

  std::atomic<double> transfer_rate_in_mbps{0.0};  // TD -> DF
  std::atomic<double> transfer_rate_out_mbps{0.0}; // DF -> Writer

  std::shared_ptr<
      dunedaq::iomanager::SenderConcept<dunedaq::datafilter::BookKeeping>>
      m_bk_sender;

  explicit BookkeepingReceiver(RunInfo &info, std::string id = "",
                               std::string bk_rx_uid = "",
                               std::string bk_tx_uid = "",
                               std::string session_name = "")
      : run_info(info), datafilter_id(std::move(id)),
        m_bk_rx_uid(std::move(bk_rx_uid)), m_bk_tx_uid(std::move(bk_tx_uid)),
        m_session_name(std::move(session_name)) {
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
    //   std::lock_guard<std::mutex> lock(queue_mutex);
    //   if (!receiver_thread) {
    //     TLOG() << "No active receiver to stop";
    //     return;
    //   }

    //   TLOG() << "Initiating receiver shutdown";
    //   stop_flag.store(true);
    //   queue_cv.notify_all();
    // }

    if (receiver_thread->joinable()) {
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
  void send_bk(dunedaq::datafilter::BookKeeping bk_info) {
    TLOG() << "Send bookkeeping info to FilterResultWriter";

    const std::string tx_uid =
        m_bk_tx_uid.empty() ? "bookkeeping1" : m_bk_tx_uid;
    m_bk_sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(tx_uid);

    if (!m_bk_sender) {
      TLOG() << "Failed to get bookkeeping sender!";
      return;
    }

    try {
      m_bk_sender->send(std::move(bk_info), Sender::s_block);
      TLOG() << "Successfully sent BookKeeping data.";

    } catch (const std::exception &e) {
      TLOG() << "Send failed: " << e.what();
    }
  }

  void receive_bk() {
    TLOG() << "Setting up bookkeeping receiver";

    const std::string rx_uid =
        m_bk_rx_uid.empty() ? "bookkeeping0" : m_bk_rx_uid;

    TLOG() << "BK: attempting to bind receiver on uid=" << rx_uid
           << " type=BookKeeping";

    auto cb_receiver =
        dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(rx_uid);
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

        send_bk(bk);
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
        // If we still have no file yet, skip (or buffer in memory)
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
