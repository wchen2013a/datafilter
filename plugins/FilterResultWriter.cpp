/**
 * @file FilterResultWriter.cpp
 *
 * Implementations of FilterResultWriter's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "FilterResultWriter.hpp"

using dunedaq::datafilter::FilterResultWriter;

namespace {

bool has_enough_space(const std::string &dir, std::uintmax_t min_free_bytes) {
  std::error_code ec;
  auto sp = std::filesystem::space(dir, ec);
  if (ec) {
    TLOG() << "StorageCheck: cannot query space for " << dir << " ("
           << ec.message() << ") -- proceeding anyway";
    return true; // don't block on query failure
  }
  if (sp.available < min_free_bytes) {
    TLOG() << "StorageCheck: STORAGE LOW -- available=" << sp.available
           << " bytes (<" << min_free_bytes << "), skipping write to " << dir;
    return false;
  }
  return true;
}
} // anonymous namespace

namespace dunedaq::datafilter {

FilterResultWriter::FilterResultWriter(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_bk_thread(std::bind(&FilterResultWriter::receive_attrs, this,
                            std::placeholders::_1)) {
  register_command("conf", &FilterResultWriter::do_conf);
  register_command("start", &FilterResultWriter::do_start);
  register_command("stop", &FilterResultWriter::do_stop);
}

void FilterResultWriter::FilterResultWriter::init(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();
  m_mcfg = mcfg;

  try {
    m_confdb = std::make_shared<dunedaq::conffwk::Configuration>(m_oksConfig);
  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  m_confdb->get<dunedaq::confmodel::Queue>(m_queues);
  m_confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  // get attributes (it is moving from TRD->DF->FRW).
  auto mdal =
      mcfg->get_dal<dunedaq::datafilter::dal::FilterResultWriter>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  m_cx = dunedaq::datafilter::ConnectionsBuilder::build_from_dal(mdal);
  TLOG() << "FRW connections: "
         << "trwriter_ctrl="
         << (m_cx.trwriter_ctrl.empty() ? "<none>" : m_cx.trwriter_ctrl.front())
         << " tr_data_rx="
         << (m_cx.tr_data_rx.empty() ? "<none>" : m_cx.tr_data_rx.front())
         << " bk_in="
         << (m_cx.bk_inputs.empty() ? "<none>" : m_cx.bk_inputs.front())
         << " bk_out="
         << (m_cx.bk_outputs.empty() ? "<none>" : m_cx.bk_outputs.front());

  m_odir = mdal->get_odir();
  m_output_h5_filename = mdal->get_output_h5_filename();
  m_min_free_bytes = static_cast<std::uintmax_t>(mdal->get_min_free_bytes());
  TLOG() << "odir " << m_odir << " output_h5_filename prefix "
         << m_output_h5_filename << " min_free_bytes=" << m_min_free_bytes;
}

void FilterResultWriter::do_conf(const data_t &) {
  TLOG() << get_name() << " do_conf()";
  dunedaq::opmonlib::TestOpMonManager opmgr;
  try {
    TLOG() << "Configure IOManager...";
    get_iomanager()->configure(m_session_name, m_queues, m_networkconnections,
                               nullptr, opmgr);
  } catch (const std::exception &e) {
    TLOG() << "Failed to configure IOManager. " << e.what();
    throw;
  }

  // --kPubSub data channels--
  // Register callbacks immediately so ZMQ SUB sockets are subscribed before
  // any other app calls do_start() and publishes data.  Callbacks deposit
  // received data into per-type prebuf queues.
  if (!m_cx.ts_data_rx.empty() && !m_ts_prebuf_rx) {
    m_ts_prebuf_rx =
        dunedaq::get_iom_receiver<timeslice_ptr_t>(m_cx.ts_data_rx.front());
    m_ts_prebuf_rx->add_callback([this](timeslice_ptr_t &ts) {
      std::lock_guard<std::mutex> lk(m_ts_prebuf_mtx);
      m_ts_prebuf.push(std::move(ts));
      m_ts_prebuf_cv.notify_one();
    });
    TLOG() << "FRW: registered TS kPubSub callback on "
           << m_cx.ts_data_rx.front();
  }

  if (!m_cx.tr_data_rx.empty() && !m_tr_prebuf_rx) {
    m_tr_prebuf_rx = dunedaq::get_iom_receiver<trigger_record_ptr_t>(
        m_cx.tr_data_rx.front());
    m_tr_prebuf_rx->add_callback([this](trigger_record_ptr_t &tr) {
      std::lock_guard<std::mutex> lk(m_tr_prebuf_mtx);
      m_tr_prebuf.push(std::move(tr));
      m_tr_prebuf_cv.notify_one();
    });
    TLOG() << "FRW: registered TR kPubSub callback on "
           << m_cx.tr_data_rx.front();
  }

  // -- kSendRecv ctrl channels (trwriter0, tswriter0)
  // IOManager creates the ZMQ PULL socket lazily on the first
  // get_iom_receiver() call.  If receive_tr/ts_single_connection() is the first
  // caller (inside do_start()), the socket is created AFTER DF has already sent
  // "write_tr"/ "write_ts" on cold start — the message may be dropped before
  // the socket exists.  Calling get_iom_receiver() here pre-creates the sockets
  // so the TCP connection is established during do_conf(), long before
  // do_start().
  if (!m_cx.trwriter_ctrl.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_cx.trwriter_ctrl.front());
    TLOG() << "FRW: pre-warmed trwriter_ctrl PULL on "
           << m_cx.trwriter_ctrl.front();
  }
  if (!m_cx.tswriter_ctrl.empty()) {
    dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
        m_cx.tswriter_ctrl.front());
    TLOG() << "FRW: pre-warmed tswriter_ctrl PULL on "
           << m_cx.tswriter_ctrl.front();
  }

  TLOG() << get_name() << ": exist do_conf()";
}

void FilterResultWriter::do_start(const data_t &) {
  // Remove any zero-byte .filtered.writing files left by a previous crashed
  // run.
  std::error_code ec;
  for (auto &entry : std::filesystem::directory_iterator(m_odir, ec)) {
    const auto &p = entry.path();
    const auto &s = p.string();
    if (s.size() > 17 && s.substr(s.size() - 17) == ".filtered.writing" &&
        std::filesystem::file_size(p, ec) == 0) {
      TLOG() << "do_start: removing stale empty partial file: " << p;
      std::filesystem::remove(p, ec);
    }
  }

  m_bk_thread.start_working_thread();

  m_running.store(true);

  // Register always-on write_tr ctrl callback before the dispatch loop so no
  // handshake is dropped in the gap between cycle N's remove_callback() and
  // cycle N+1's add_callback() (same pattern as m_tr_prebuf_rx for kPubSub).
  if (!m_cx.trwriter_ctrl.empty()) {
    m_write_tr_ctrl_rx =
        dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
            m_cx.trwriter_ctrl.front());
    m_write_tr_ctrl_rx->add_callback(
        [this](dunedaq::datafilter::Handshake msg) {
          if (msg.msg_id == "write_tr") {
            std::lock_guard<std::mutex> lk(m_write_tr_prebuf_mtx);
            m_write_tr_prebuf.push(std::move(msg));
            m_write_tr_prebuf_cv.notify_one();
          }
        });
  }

  while (m_running.load()) {
    // Wait until DF signals a new dispatch via bookkeeping1.
    {
      std::unique_lock<std::mutex> lk(m_dispatch_mutex);
      m_dispatch_cv.wait(lk, [this] {
        return m_dispatch_ready.load(std::memory_order_acquire) ||
               !m_running.load();
      });
      if (!m_running.load())
        break;
      m_dispatch_ready.store(false, std::memory_order_release);
    }
    TLOG() << "FRW: dispatch gate passed — starting TR/TS receive cycle";
    std::thread tr_thread([this] {
      if (!m_cx.tr_data_rx.empty())
        receive_tr_single_connection();
    });
    std::thread ts_thread([this] {
      if (!m_cx.ts_data_rx.empty())
        receive_ts_single_connection();
    });
    tr_thread.join();
    ts_thread.join();
  }
  TLOG() << "FRW: do_start() dispatch loop exiting";
}
void FilterResultWriter::do_stop(const data_t &) {
  m_running.store(false);
  m_dispatch_cv.notify_all();

  // Wake drain loops so they see m_running==false and exit.
  // Callbacks remain registered so ZMQ SUB stays subscribed for the next run.
  m_ts_prebuf_cv.notify_all();
  m_tr_prebuf_cv.notify_all();
  m_write_tr_prebuf_cv.notify_all();
  {
    std::lock_guard<std::mutex> lk(m_ts_prebuf_mtx);
    while (!m_ts_prebuf.empty())
      m_ts_prebuf.pop();
  }
  {
    std::lock_guard<std::mutex> lk(m_tr_prebuf_mtx);
    while (!m_tr_prebuf.empty())
      m_tr_prebuf.pop();
  }
  {
    std::lock_guard<std::mutex> lk(m_write_tr_prebuf_mtx);
    while (!m_write_tr_prebuf.empty())
      m_write_tr_prebuf.pop();
  }
  if (m_write_tr_ctrl_rx) {
    m_write_tr_ctrl_rx->remove_callback();
    m_write_tr_ctrl_rx.reset();
  }

  m_bk_thread.stop_working_thread();
}

std::string
FilterResultWriter::generate_hdf5file_pathname(std::string file_pathname_prefix,
                                               int run_number, int file_index,
                                               int trigger_number) {
  std::ostringstream filename_oss;
  filename_oss << file_pathname_prefix << "_" << std::setw(6)
               << std::setfill('0') << run_number << "_" << std::setw(4)
               << std::setfill('0') << file_index << "_" << trigger_number;
  return filename_oss.str();
}

dunedaq::hdf5libs::HDF5FileLayoutParameters create_file_layout_params() {
  dunedaq::hdf5libs::HDF5PathParameters params_tpc;
  params_tpc.detector_group_type = "Detector_Readout";
  params_tpc.detector_group_name = "TPC";
  params_tpc.element_name_prefix = "Link";
  params_tpc.digits_for_element_number = 5;

  std::vector<dunedaq::hdf5libs::HDF5PathParameters> param_list;
  param_list.push_back(params_tpc);

  dunedaq::hdf5libs::HDF5FileLayoutParameters layout_params;
  layout_params.path_params_list = param_list;
  layout_params.record_name_prefix = "TriggerRecord";
  layout_params.digits_for_record_number = 6;
  layout_params.digits_for_sequence_number = 0;
  layout_params.record_header_dataset_name = "TriggerRecordHeader";

  return layout_params;
}

void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
  TLOG() << "BookKeeping receive_attrs starting";

  if (m_cx.bk_inputs.empty()) {
    TLOG() << "FRW: no bookkeeping inputs configured; skipping receive_attrs";
    return;
  }
  const std::string &bk_rx_uid = m_cx.bk_inputs.front();

  auto receiver =
      dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(bk_rx_uid);
  if (!receiver) {
    TLOG() << "Failed to get BookKeeping receiver 'bookkeeping1'";
    return;
  }

  std::function<void(dunedaq::datafilter::BookKeeping &)> cb =
      [&](dunedaq::datafilter::BookKeeping bk) {
        // Update run_number from every BK so TS-only mode gets the right value.
        if (bk.run_number > 0)
          m_run_number.store(bk.run_number);

        // Find "file_index" and update (keep this fast)
        for (const auto &kv : bk.file_attributes_info) {
          if (kv.first == "file_index") {
            try {
              const int idx = std::stoi(kv.second);
              FilterResultWriter::set_file_index(idx);
            } catch (const std::exception &e) {
              TLOG() << "Invalid file_index value: '" << kv.second << "' ("
                     << e.what() << ")";
            }
            break;
          }
        }

        TLOG_DEBUG(1) << "BookKeeping from " << bk.from_id
                      << " run=" << bk.run_number
                      << " file_index=" << FilterResultWriter::get_file_index();

        // Signal do_start() that a new dispatch cycle has begun.
        {
          std::lock_guard<std::mutex> lk(m_dispatch_mutex);
          m_dispatch_ready.store(true, std::memory_order_release);
        }
        m_dispatch_cv.notify_one();
        TLOG() << "FRW: dispatch gate opened (bookkeeping1 received from "
               << bk.from_id << ")";
      };

  TLOG() << "Registering BookKeeping callback";
  receiver->add_callback(cb);

  // Keep callback alive for the entire run
  while (running.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  TLOG() << "Removing BookKeeping callback";
  receiver->remove_callback();

  TLOG() << "BookKeeping receive_attrs exiting";
}

void FilterResultWriter::receive_tr_single_connection() {
  bool handshake_done = false;

  // Reset per-call state so a stale m_num_messages from a previous call does
  // not carry into this call's bookkeeping.
  m_num_messages.store(0);

  // Drain write_tr from the always-on prebuf (registered in do_start()).
  {
    std::unique_lock<std::mutex> lk(m_write_tr_prebuf_mtx);
    handshake_done =
        m_write_tr_prebuf_cv.wait_for(lk, std::chrono::seconds(10), [this] {
          return !m_write_tr_prebuf.empty();
        });
    if (handshake_done) {
      auto &msg = m_write_tr_prebuf.front();
      m_num_messages = msg.total_tr;
      TLOG() << "FilterResultWriter: TR receiver callback: " << msg.msg_id;
      m_write_tr_prebuf.pop();
    }
  }

  if (!handshake_done) {
    TLOG() << "FRW: no write_tr handshake received; skipping";
    return;
  }

  HDF5FileLayoutParameters fl_pars = create_file_layout_params();
  auto srcid_geoid_map = create_srcid_geoid_map();

  // Use a single connection for all TRs
  // std::string single_connection = "conn_A1_G0_C0_";
  // TLOG() << "Listening for ALL TRs on single connection: " <<
  // single_connection; TLOG() << "Expecting " << m_num_messages << " TRs
  // total";

  // Use a single connection for all TRs (discovered)
  if (m_cx.tr_data_rx.empty()) {
    TLOG() << "FRW: no TR data inputs configured; cannot receive TRs";
    return;
  }
  const std::string &single_connection = m_cx.tr_data_rx.front();
  TLOG() << "Listening for ALL TRs on single connection: " << single_connection;

  std::atomic<size_t> total_msgs_received{0};
  size_t total_expected = m_num_messages;

  struct TRWriteRecord {
    std::string pathname;
    size_t trigger_number;
    size_t trigger_timestamp;
  };
  std::mutex pathnames_mutex;
  std::vector<TRWriteRecord> written_trs;

  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);

  // Drain from the always-on prebuf (registered in do_start() before the
  // dispatch gate) so data received before this call is not lost.
  TLOG() << "Waiting for " << total_expected << " TRs from prebuf...";
  const auto tr_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(120);
  while (m_running.load()) {
    trigger_record_ptr_t tr;
    {
      std::unique_lock<std::mutex> lk(m_tr_prebuf_mtx);
      bool got = m_tr_prebuf_cv.wait_until(lk, tr_deadline, [this] {
        return !m_tr_prebuf.empty() || !m_running.load();
      });
      if (!m_running.load() || m_tr_prebuf.empty())
        break;
      tr = std::move(m_tr_prebuf.front());
      m_tr_prebuf.pop();
    }

    m_trigger_timestamp =
        tr->get_fragments_ref().at(0)->get_trigger_timestamp();
    m_trigger_number = tr->get_fragments_ref().at(0)->get_trigger_number();
    m_run_number.store(tr->get_fragments_ref().at(0)->get_run_number());
    size_t file_index = get_file_index();

    size_t current_total = ++total_msgs_received;

    TLOG() << "Received TR " << current_total << "/" << total_expected
           << " - run: " << m_run_number.load()
           << ", trigger: " << m_trigger_number
           << ", file_index: " << file_index;

    // Write each TR to its own file
    std::string app_name = "test";
    std::string file_pathname_prefix = m_odir + "/" + m_output_h5_filename;
    std::string file_base =
        generate_hdf5file_pathname(file_pathname_prefix, m_run_number.load(),
                                   file_index, m_trigger_number);
    std::string writing_pathname = file_base + ".filtered.writing";
    std::string final_pathname = file_base + ".filtered.hdf5";

    TLOG() << "Writing TR " << current_total << "/" << total_expected << " to "
           << writing_pathname;

    if (!has_enough_space(m_odir, m_min_free_bytes)) {
      TLOG() << "Skipping TR write — insufficient storage in " << m_odir;
      return;
    }

    unsigned compression_level = 0;

    try {
      std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
          writing_pathname, m_run_number.load(), m_file_index, app_name,
          fl_pars, srcid_geoid_map, compression_level, ""));

      h5file_ptr->write(*tr);
      h5file_ptr.reset();

      std::filesystem::rename(writing_pathname, final_pathname);
      TLOG() << "Successfully wrote TR " << current_total
             << " with trigger_number " << m_trigger_number << " -> "
             << final_pathname;

      {
        std::lock_guard<std::mutex> lock(pathnames_mutex);
        written_trs.push_back(
            {final_pathname, m_trigger_number, m_trigger_timestamp});
      }

    } catch (const std::exception &e) {
      TLOG() << "ERROR writing TR: " << e.what();

      dunedaq::datafilter::BookKeeping bk_error("bookkeeping0");
      bk_error.entry_id =
          time_point_to_string(std::chrono::system_clock::now());
      bk_error.conn_id = single_connection;
      bk_error.from_id = "FilterResultWriter";
      bk_error.tr_status = to_string(TRStatus::kWriteFailed);
      bk_error.run_number = m_run_number.load();
      bk_error.tr_header_info.push_back(
          {"run_number", std::to_string(m_run_number.load())});
      bk_error.tr_header_info.push_back(
          {"trigger_number", std::to_string(m_trigger_number)});
      bk_error.tr_header_info.push_back({"error", e.what()});

      auto bookkeeping_sender =
          dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
              "bookkeeping0");
      bookkeeping_sender->send(std::move(bk_error), Sender::s_block);
    }

    // Check if all expected messages received
    if (current_total >= total_expected) {
      TLOG() << "All " << total_expected << " TRs received and written";
      break;
    }
  }

  if (total_msgs_received.load() < total_expected) {
    TLOG() << "WARNING: Timeout or stop before all TRs received. Got "
           << total_msgs_received.load() << "/" << total_expected;
  }

  if (total_expected > 0) {
    // Send final bookkeeping
    TLOG() << "Send final bookkeeping info to datafilter server";
    TLOG() << "Total files written: " << written_trs.size();
    TLOG() << "Expected: " << total_expected
           << ", Actual: " << total_msgs_received.load();

    const size_t actually_received = total_msgs_received.load();
    const size_t actually_written = written_trs.size();

    if (actually_received < total_expected) {
      TLOG() << "FRW: received " << actually_received << "/" << total_expected
             << " TRs — " << (total_expected - actually_received)
             << " filtered upstream by DataFilter (expected)";
    }
    if (actually_written < actually_received) {
      TLOG() << "WARNING: wrote only " << actually_written << "/"
             << actually_received << " TRs received — write failures!";
    }

    dunedaq::datafilter::BookKeeping final_bk_info("bookkeeping0");
    final_bk_info.entry_id =
        time_point_to_string(std::chrono::system_clock::now());
    final_bk_info.conn_id = single_connection;
    final_bk_info.from_id = "FilterResultWriter";
    // kFileCompleted if every TR we received was written successfully.
    // Upstream filtering (DF dropping TRs before forwarding) is normal and
    // does NOT constitute a write failure — use actually_received, not
    // total_expected (which reflects TRD's original dispatch count).
    final_bk_info.tr_status = (actually_written >= actually_received)
                                  ? to_string(TRStatus::kFileCompleted)
                                  : to_string(TRStatus::kWriteFailed);
    final_bk_info.run_number = m_run_number.load();
    final_bk_info.tr_header_info.push_back(
        {"run_number", std::to_string(m_run_number.load())});
    final_bk_info.tr_header_info.push_back(
        {"total_trs_written", std::to_string(actually_written)});
    final_bk_info.tr_header_info.push_back(
        {"trs_received_from_df", std::to_string(actually_received)});
    final_bk_info.tr_header_info.push_back(
        {"trs_dispatched_by_trd", std::to_string(total_expected)});
    final_bk_info.tr_header_info.push_back(
        {"trs_filtered_upstream",
         std::to_string(total_expected > actually_received
                            ? total_expected - actually_received
                            : 0)});

    for (size_t i = 0; i < actually_written; ++i) {
      final_bk_info.tr_header_info.push_back(
          {"file_" + std::to_string(i), written_trs[i].pathname});
      final_bk_info.tr_header_info.push_back(
          {"trigger_number_" + std::to_string(i),
           std::to_string(written_trs[i].trigger_number)});
      final_bk_info.tr_header_info.push_back(
          {"trigger_timestamp_" + std::to_string(i),
           std::to_string(written_trs[i].trigger_timestamp)});
    }

    auto final_bookkeeping_sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
            "bookkeeping0");
    final_bookkeeping_sender->send(std::move(final_bk_info), Sender::s_block);
  }

  m_num_messages = 0;
  TLOG_DEBUG(5) << "receive_tr_single_connection() done";
}

void FilterResultWriter::receive_ts_single_connection() {
  if (m_cx.ts_data_rx.empty()) {
    TLOG_DEBUG(7) << "FRW: no TS data inputs configured; skipping TS receive";
    return;
  }

  std::mutex cv_mutex;
  std::condition_variable cv;
  bool handshake_done = false;
  std::atomic<size_t> ts_expected{0};

  // Wait for "write_ts" control if tswriter_ctrl is configured
  if (!m_cx.tswriter_ctrl.empty()) {
    const std::string &tsw_ctrl_uid = m_cx.tswriter_ctrl.front();
    auto ctrl_rx =
        dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(tsw_ctrl_uid);
    std::function<void(dunedaq::datafilter::Handshake)> ctrl_cb =
        [&](dunedaq::datafilter::Handshake msg) {
          if (msg.msg_id == "write_ts") {
            ts_expected.store(msg.total_tr);
            std::lock_guard<std::mutex> lock(cv_mutex);
            handshake_done = true;
            cv.notify_one();
          }
        };
    ctrl_rx->add_callback(ctrl_cb);
    {
      std::unique_lock<std::mutex> lock(cv_mutex);
      cv.wait_for(lock, std::chrono::seconds(10),
                  [&] { return handshake_done; });
    }
    ctrl_rx->remove_callback();
    if (!handshake_done) {
      TLOG() << "FRW: no write_ts handshake received; skipping";
      return;
    }
  }

  // TimeSlice file layout
  dunedaq::hdf5libs::HDF5FileLayoutParameters ts_fl_pars;
  dunedaq::hdf5libs::HDF5PathParameters params_tpc;
  params_tpc.detector_group_type = "Detector_Readout";
  params_tpc.detector_group_name = "TPC";
  params_tpc.element_name_prefix = "Link";
  params_tpc.digits_for_element_number = 5;
  ts_fl_pars.path_params_list.push_back(params_tpc);
  ts_fl_pars.record_name_prefix = "TimeSlice";
  ts_fl_pars.digits_for_record_number = 6;
  ts_fl_pars.digits_for_sequence_number = 0;
  ts_fl_pars.record_header_dataset_name = "TimeSliceHeader";

  auto srcid_geoid_map = create_srcid_geoid_map();

  const std::string &ts_conn = m_cx.ts_data_rx.front();
  TLOG() << "Listening for TimeSlices on: " << ts_conn << " expecting "
         << ts_expected.load();

  std::atomic<size_t> ts_received{0};
  std::atomic<size_t> ts_written{0};
  std::vector<std::string> written_ts_pathnames;

  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);

  // Drain from the always-on prebuf (registered in do_start() before the
  // dispatch gate) so data received before this function was called is not
  // lost.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(120);
  while (m_running.load()) {
    timeslice_ptr_t ts;
    {
      std::unique_lock<std::mutex> lk(m_ts_prebuf_mtx);
      bool got = m_ts_prebuf_cv.wait_until(lk, deadline, [this] {
        return !m_ts_prebuf.empty() || !m_running.load();
      });
      if (!m_running.load() || m_ts_prebuf.empty())
        break;
      ts = std::move(m_ts_prebuf.front());
      m_ts_prebuf.pop();
    }

    ++ts_received;
    auto ts_number = ts->get_header().timeslice_number;
    size_t current = ts_received.load();

    TLOG() << "Received TS " << current << "/" << ts_expected.load()
           << " ts_number=" << ts_number;

    // Prefix with "_ts" to keep TS filenames distinct from TR filenames,
    // which use the same run/file_index/sequence scheme.
    std::string file_pathname_prefix =
        m_odir + "/" + m_output_h5_filename + "_ts";
    std::string file_base = generate_hdf5file_pathname(
        file_pathname_prefix, m_run_number.load(), get_file_index(), ts_number);
    std::string writing_pathname = file_base + ".filtered.writing";
    std::string final_pathname = file_base + ".filtered.hdf5";

    if (!has_enough_space(m_odir, m_min_free_bytes)) {
      TLOG() << "Skipping TS write — insufficient storage in " << m_odir;
    } else {
      unsigned compression_level = 0;
      try {
        std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
            writing_pathname, m_run_number.load(), get_file_index(), "test",
            ts_fl_pars, srcid_geoid_map, compression_level, ""));
        h5file_ptr->write(*ts);
        h5file_ptr.reset();
        std::filesystem::rename(writing_pathname, final_pathname);
        ++ts_written;
        written_ts_pathnames.push_back(final_pathname);
        TLOG() << "Successfully wrote TS " << current
               << " ts_number=" << ts_number << " -> " << final_pathname;
      } catch (const std::exception &e) {
        TLOG() << "ERROR writing TS: " << e.what();
      }
    }

    if (ts_expected.load() > 0 && current >= ts_expected.load()) {
      TLOG() << "All " << ts_expected.load() << " TSs received and written";
      break;
    }
  }

  // Notify DF of TS batch completion; DF's BookkeepingReceiver translates this
  // to kReRecorded and forwards to TRD on bookkeeping2.
  if (!m_cx.bk_outputs.empty()) {
    const bool all_written =
        (ts_expected.load() > 0) && (ts_written.load() >= ts_expected.load());
    const auto ts_status = all_written ? to_string(TRStatus::kFileCompleted)
                                       : to_string(TRStatus::kWriteFailed);
    dunedaq::datafilter::BookKeeping ts_bk(m_cx.bk_outputs.front());
    ts_bk.entry_id = time_point_to_string(std::chrono::system_clock::now());
    ts_bk.from_id = "FilterResultWriter";
    ts_bk.run_number = m_run_number.load();
    ts_bk.tr_status = ts_status;
    ts_bk.tr_header_info.push_back(
        {"total_ts_written", std::to_string(ts_written.load())});
    ts_bk.tr_header_info.push_back(
        {"expected_ts", std::to_string(ts_expected.load())});
    for (size_t i = 0; i < written_ts_pathnames.size(); ++i) {
      ts_bk.tr_header_info.push_back(
          {"ts_file_" + std::to_string(i), written_ts_pathnames[i]});
    }
    try {
      auto bk_sender =
          dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
              m_cx.bk_outputs.front());
      bk_sender->send(std::move(ts_bk), Sender::s_block);
      TLOG() << "FRW: sent TS completion BK (" << ts_status << ") to DF"
             << " ts_received=" << ts_received.load()
             << " ts_expected=" << ts_expected.load();
    } catch (const std::exception &e) {
      TLOG() << "FRW: TS completion BK send failed: " << e.what();
    }
  }

  TLOG_DEBUG(5) << "receive_ts_single_connection() done";
}

// DF should alway send next_tr, so it is not used here. It will be removed in
// next cleanup.
void FilterResultWriter::send_next_tr() {
  bool handshake_done = false;

  std::atomic<unsigned int> sent_cnt = 0;

  auto sender_next_tr =
      dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>("trdispatcher1");

  // std::chrono::milliseconds timeout(100);
  dunedaq::datafilter::Handshake sent_t1("next_tr");
  // sender_next_tr->send(std::move(sent_t1), timeout);
  sender_next_tr->send(std::move(sent_t1), Sender::s_block);
}

void FilterResultWriter::generate_opmon_data() {
  dunedaq::datafilter::opmon::FilterResultWriterInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

void FilterResultWriter::receive_attrs_test() {
  using BK = dunedaq::datafilter::BookKeeping;
  TLOG() << "receive_attrs_test: starting std::thread receiver";

  auto receiver = dunedaq::get_iom_receiver<BK>("bookkeeping1");
  if (!receiver) {
    TLOG() << "receive_attrs_test: failed to get 'bookkeeping1' receiver";
    m_attrs_test_running.store(false, std::memory_order_release);
    return;
  }

  std::function<void(BK &)> cb = [&](BK bk) {
    // Extract "file_index"
    for (const auto &kv : bk.file_attributes_info) {
      if (kv.first == "file_index") {
        try {
          const int idx = std::stoi(kv.second);
          // Ensure these are thread-safe (atomic or mutex-protected)
          FilterResultWriter::set_file_index(idx);
        } catch (const std::exception &e) {
          TLOG() << "receive_attrs_test: invalid file_index '" << kv.second
                 << "' (" << e.what() << ")";
        }
        break;
      }
    }

    TLOG_DEBUG(1) << "BookKeeping from " << bk.from_id
                  << " run=" << bk.run_number
                  << " file_index=" << FilterResultWriter::get_file_index();
  };

  TLOG() << "receive_attrs_test: registering callback";
  receiver->add_callback(cb);

  // Keep callback alive for entire run; block here until stop requested
  {
    std::unique_lock<std::mutex> lk(m_attrs_test_mtx);
    m_attrs_test_cv.wait(lk, [this] {
      return !m_attrs_test_running.load(std::memory_order_relaxed);
    });
  }

  TLOG() << "receive_attrs_test: removing callback and exiting";
  receiver->remove_callback();
}

// Using thread instead. It is not used
void FilterResultWriter::start_receive_attrs_test_thread() {
  // prevent double-start
  bool was_running =
      m_attrs_test_running.exchange(true, std::memory_order_acq_rel);
  if (was_running)
    return;

  m_attrs_test_thread =
      std::thread(&FilterResultWriter::receive_attrs_test, this);
}

void FilterResultWriter::stop_receive_attrs_test_thread() {
  bool was_running =
      m_attrs_test_running.exchange(false, std::memory_order_acq_rel);
  if (!was_running)
    return;

  // Wake the thread if it's waiting
  m_attrs_test_cv.notify_all();

  if (m_attrs_test_thread.joinable())
    m_attrs_test_thread.join();
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::FilterResultWriter)
