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
#include <optional>

using dunedaq::datafilter::FilterResultWriter;

namespace dunedaq::datafilter {

FilterResultWriter::FilterResultWriter(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_thread(
          std::bind(&FilterResultWriter::do_work, this, std::placeholders::_1)),
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

  // get TRDispatcher attributes.
  auto mdal =
      mcfg->get_dal<dunedaq::datafilter::dal::FilterResultWriter>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }
  m_odir = mdal->get_odir();
  m_output_h5_filename = mdal->get_output_h5_filename();
  TLOG() << "odir " << m_odir << "output_h5_filename prefix"
         << m_output_h5_filename;
}

void FilterResultWriter::do_conf(const data_t &) {
  // auto iom = iomanager::IOManager::get();
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

  TLOG() << get_name() << ": exist do_conf()";
}

void FilterResultWriter::do_start(const data_t &) {
  m_bk_thread.start_working_thread();
  // m_thread.start_working_thread();
  while (true) {
    receive_tr_single_connection();
  }
}
void FilterResultWriter::do_stop(const data_t &) {
  // m_thread.stop_working_thread();
  m_bk_thread.stop_working_thread();
}

void FilterResultWriter::do_work(std::atomic<bool> &running) {
  std::mutex work_mutex;
  std::condition_variable work_cv;

  while (running.load()) {
    // receive_tr();
    receive_tr_single_connection();

    std::unique_lock<std::mutex> lock(work_mutex);
    work_cv.wait_for(lock, std::chrono::seconds(1), [&]() {
      return !running.load(); // check for new work availability
    });
  }
}

std::string
FilterResultWriter::generate_hdf5file_pathname(std::string file_pathname_prefix,
                                               int run_number, int file_index,
                                               int trigger_number) {
  std::ostringstream filename_oss;
  filename_oss << file_pathname_prefix << "_" << std::setw(6)
               << std::setfill('0') << run_number << "_" << std::setw(4)
               << std::setfill('0') << file_index << "_" << trigger_number
               << ".hdf5";
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

// void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
//   TLOG() << "Receive attrs==================================";
//   std::atomic<unsigned int> received_cnt{0};
//   std::atomic<bool> is_done{false};

//   auto cb_receiver =
//       dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
//           "bookkeeping1");
//   if (!cb_receiver) {
//     TLOG() << "Failed to get bookkeeping receiver";
//     return;
//   }

//   std::function<void(dunedaq::datafilter::BookKeeping)> str_receiver_cb =
//       [&](dunedaq::datafilter::BookKeeping bk) {
//         ++received_cnt;
//         if (received_cnt == 1) {
//           if (auto it = std::find_if(
//                   bk.file_attributes_info.begin(),
//                   bk.file_attributes_info.end(),
//                   [](const auto &p) { return p.first == "file_index"; });
//               it != bk.file_attributes_info.end()) {
//             // m_file_index = it->second;
//             dunedaq::datafilter::FilterResultWriter::set_file_index(
//                 std::stoi(it->second));
//           }
//         }
//         TLOG() << "Processing bookkeeping attributes # " << received_cnt
//                << " from " << bk.from_id << " (Run: " << bk.run_number
//                << " file index: " << get_file_index() << ")";
//       };

//   TLOG() << "Registering callback...";
//   cb_receiver->add_callback(str_receiver_cb);
//   TLOG() << "Callback registered, entering main loop";
//   while (!is_done) {
//     if (received_cnt == 1) {
//       TLOG() << "Check received_cnt" << received_cnt;
//       is_done = true;
//     }
//   }
//   TLOG() << "Cleaning up receiver";
//   cb_receiver->remove_callback();
// }

// void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
//   TLOG() << "BookKeeping attrs_thread starting";

//   auto cb_receiver =
//       dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
//           "bookkeeping1");
//   if (!cb_receiver) {
//     TLOG() << "Failed to get bookkeeping receiver";
//     return;
//   }

//   // Keep the callback tiny: parse + store
//   auto cb =
//       [this](dunedaq::datafilter::BookKeeping bkex(bk)) {
//         // If you have set_file_index()/get_file_index() methods, ensure
//         they
//         // use atomics internally.
//         s_file_index.store(*idx, std::memory_order_release);
//       }

//       TLOG_DEBUG(1)
//       << "BookKeeping from " << bk.from_id << " run=" << bk.run_number
//       << " file_index=" << s_file_index.load(std::memory_order_acquire);
// };

// TLOG() << "Registering bookkeeping callback";
// cb_receiver->add_callback(cb);

// // Stay alive for the entire run; don’t spin—sleep a little and check
// 'running' while (running.load(std::memory_order_relaxed)) {
//   std::this_thread::sleep_for(std::chrono::milliseconds(200));
// }

// TLOG() << "Removing bookkeeping callback";
// cb_receiver->remove_callback();

// TLOG() << "BookKeeping receive_attrs exiting";
// }

// If your set/get are not already thread-safe, back them with an atomic.
// (Comment this out if you already have thread-safe
// set_file_index/get_file_index.)
// static std::atomic<int> g_file_index_atomic{0};
// inline void set_file_index_atomic(int v) {
//   g_file_index_atomic.store(v, std::memory_order_release);
// }
// inline int get_file_index_atomic() {
//   return g_file_index_atomic.load(std::memory_order_acquire);
// }

void FilterResultWriter::receive_attrs(std::atomic<bool> &running) {
  TLOG() << "BookKeeping receive_attrs starting";

  auto receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
      "bookkeeping1");
  if (!receiver) {
    TLOG() << "Failed to get BookKeeping receiver 'bookkeeping1'";
    return;
  }

  std::function<void(dunedaq::datafilter::BookKeeping &)> cb =
      [&](dunedaq::datafilter::BookKeeping bk) {
        // Find "file_index" and update (keep this fast)
        for (const auto &kv : bk.file_attributes_info) {
          if (kv.first == "file_index") {
            try {
              const int idx = std::stoi(kv.second);
              FilterResultWriter::set_file_index(
                  idx); // <-- ensure this is thread-safe
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

// void FilterResultWriter::receive_tr(size_t run_number1) {
//   // Use condition variable instead of busy-wait
//   std::mutex cv_mutex;
//   std::condition_variable cv;
//   bool handshake_done = false;
//   std::atomic<unsigned int> received_cnt = 0;

//   auto cb_receiver =
//       dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("trwriter0");
//   std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
//       [&](dunedaq::datafilter::Handshake msg) {
//         if (msg.msg_id == "write_tr") {
//           ++received_cnt;
//           std::lock_guard<std::mutex> lock(cv_mutex);
//           handshake_done = true;
//           cv.notify_one();
//         }
//         TLOG_DEBUG(5) << "FilterResultWriter: TR receiver callback: "
//                       << msg.msg_id;
//       };

//   cb_receiver->add_callback(str_receiver_cb);

//   // Wait with timeout instead of busy-waiting
//   {
//     std::unique_lock<std::mutex> lock(cv_mutex);
//     cv.wait_for(lock, std::chrono::seconds(30), [&] { return handshake_done;
//     });
//   }

//   if (!handshake_done) {
//     TLOG() << "Warning: Handshake timeout";
//   }

//   TLOG_DEBUG(5) << "Setting up TRWriterInfo objects";
//   for (size_t group = 0; group < m_num_groups; ++group) {
//     for (size_t conn = 0; conn < m_num_connections_per_group; ++conn) {
//       subscribers.push_back(std::make_shared<SubscriberInfo>(group, conn));
//     }
//   }

//   dunedaq::datafilter::time_point_to_string time_point_to_string(
//       dunedaq::datafilter::Precision::NANOSECONDS);
//   auto t1 = std::chrono::system_clock::now();
//   dunedaq::datafilter::BookKeeping bk_info("bookkeeping0");
//   bk_info.entry_id = time_point_to_string(t1);
//   bk_info.conn_id = m_init_connection;
//   bk_info.from_id = "FilterResultWriter";

//   HDF5FileLayoutParameters fl_pars = create_file_layout_params();
//   auto srcid_geoid_map = create_srcid_geoid_map();

//   std::atomic<std::chrono::steady_clock::time_point> last_received =
//       std::chrono::steady_clock::now();

//   TLOG_DEBUG(5) << "Adding callbacks for each subscriber";
//   for (auto subscriber : subscribers) {
//     TLOG() << "subscriber ====> " << subscriber;
//   }

//   // Use condition variable for completion signaling
//   std::mutex completion_mutex;
//   std::condition_variable completion_cv;

//   // Collect all written file pathnames for bookkeeping
//   std::mutex pathnames_mutex;
//   std::vector<std::string> written_pathnames;

//   // Switch for now to sequential execution to avoid thread creation overhead
//   // We will test the benefit using the parallel, par_unseq, for many
//   // subscribers later.
//   for (auto info : subscribers) {
//     auto recv_proc = [=, &last_received, &completion_mutex,
//                       &completion_cv](trigger_record_ptr_t &tr) {
//       m_trigger_timestamp =
//           tr->get_fragments_ref().at(0)->get_trigger_timestamp();
//       m_trigger_number = tr->get_fragments_ref().at(0)->get_trigger_number();
//       m_run_number = tr->get_fragments_ref().at(0)->get_run_number();
//       size_t file_index = get_file_index();

//       TLOG() << "run_number " << m_run_number << " file index: " <<
//       file_index
//              << ", trigger number: " << m_trigger_number;

//       info->msgs_received++;
//       last_received = std::chrono::steady_clock::now();

//       // Write EVERY trigger record to its own file
//       std::string app_name = "test";
//       std::string file_pathname_prefix = m_odir + "/" + m_output_h5_filename;

//       // Generate unique filename for THIS trigger record
//       std::string file_pathname = generate_hdf5file_pathname(
//           file_pathname_prefix, m_run_number, file_index, m_trigger_number);

//       TLOG() << "Writing TR " << info->msgs_received << "/" << m_num_messages
//              << " to " << file_pathname;

//       unsigned compression_level = 0;

//       try {
//         std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
//             file_pathname, m_run_number, m_file_index, app_name, fl_pars,
//             srcid_geoid_map, compression_level));

//         h5file_ptr->write(*tr);
//         h5file_ptr.reset();

//         TLOG() << "Successfully wrote TR " << info->msgs_received
//                << " with trigger_number " << m_trigger_number;
//       } catch (const std::exception &e) {
//         TLOG() << "ERROR writing TR: " << e.what();
//       }

//       // Mark complete when ALL messages received
//       if (info->msgs_received >= m_num_messages) {
//         TLOG() << "All " << m_num_messages
//                << " TRs received and written for subscriber";
//         info->complete = true;

//         // Notify that a subscriber completed
//         {
//           std::lock_guard<std::mutex> lock(completion_mutex);
//           completion_cv.notify_one();
//         }
//       }
//     };

//     m_init_connection = "conn_A1_G0_C0_";
//     auto before_receiver = std::chrono::steady_clock::now();
//     auto receiver = dunedaq::get_iom_receiver<
//         std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>(
//         m_init_connection);
//     auto after_receiver = std::chrono::steady_clock::now();
//     receiver->add_callback(recv_proc);
//     auto after_callback = std::chrono::steady_clock::now();
//     info->get_receiver_time =
//         std::chrono::duration_cast<std::chrono::milliseconds>(after_receiver
//         -
//                                                               before_receiver);
//     info->add_callback_time =
//         std::chrono::duration_cast<std::chrono::milliseconds>(after_callback
//         -
//                                                               after_receiver);
//   }

//   // ACK to datafilter that we successfully received the TR
//   TLOG() << "Send bookkeeping info to datafilter server";
//   bk_info.tr_status = "received";
//   bk_info.run_number = m_run_number;
//   bk_info.tr_header_info.push_back(
//       {"run_number", std::to_string(m_run_number)});
//   bk_info.tr_header_info.push_back(
//       {"trigger_number", std::to_string(m_trigger_number)});
//   bk_info.tr_header_info.push_back({"tr_writer_pathname", m_ofile_pathname});

//   auto init_bookkeeping_sender =
//       dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>("bookkeeping0");
//   init_bookkeeping_sender->send(std::move(bk_info), Sender::s_block);

//   TLOG_DEBUG(5) << "Starting wait loop for receives to complete";

//   // Wait with condition variable instead of busy-waiting
//   size_t expected_complete = m_num_groups * m_num_connections_per_group;
//   {
//     std::unique_lock<std::mutex> lock(completion_mutex);
//     completion_cv.wait(lock, [&]() {
//       size_t recvrs_done = 0;
//       for (auto &sub : subscribers) {
//         if (sub->complete.load())
//           recvrs_done++;
//       }
//       TLOG_DEBUG(6) << "Done: " << recvrs_done
//                     << ", expected: " << expected_complete;
//       return recvrs_done >= expected_complete;
//     });
//   }

//   TLOG_DEBUG(5) << "Removing callbacks";
//   for (auto &info : subscribers) {
//     auto receiver =
//         dunedaq::get_iom_receiver<trigger_record_ptr_t>(m_init_connection);
//     receiver->remove_callback();
//   }

//   auto cb_receiver1 =
//       dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("trwriter0");
//   cb_receiver1->remove_callback();

//   subscribers.clear();
//   TLOG_DEBUG(5) << "receive() done";
// }

void FilterResultWriter::receive_tr() {
  std::mutex cv_mutex;
  std::condition_variable cv;
  bool handshake_done = false;

  auto cb_receiver =
      dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("trwriter0");
  std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
      [&](dunedaq::datafilter::Handshake msg) {
        if (msg.msg_id == "write_tr") {
          m_num_messages = msg.total_tr;
          std::lock_guard<std::mutex> lock(cv_mutex);
          handshake_done = true;
          cv.notify_one();
        }
        TLOG_DEBUG(5) << "FilterResultWriter: TR receiver callback: "
                      << msg.msg_id;
      };

  cb_receiver->add_callback(str_receiver_cb);

  {
    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait_for(lock, std::chrono::seconds(30), [&] { return handshake_done; });
  }

  if (!handshake_done) {
    TLOG() << "Warning: Handshake timeout";
  }

  TLOG_DEBUG(5) << "Setting up TRWriterInfo objects";
  for (size_t group = 0; group < m_num_groups; ++group) {
    for (size_t conn = 0; conn < m_num_connections_per_group; ++conn) {
      subscribers.push_back(std::make_shared<SubscriberInfo>(group, conn));
    }
  }

  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);
  auto t1 = std::chrono::system_clock::now();
  dunedaq::datafilter::BookKeeping bk_info("bookkeeping0");
  bk_info.entry_id = time_point_to_string(t1);
  bk_info.conn_id = m_init_connection;
  bk_info.from_id = "FilterResultWriter";

  HDF5FileLayoutParameters fl_pars = create_file_layout_params();
  auto srcid_geoid_map = create_srcid_geoid_map();

  std::atomic<std::chrono::steady_clock::time_point> last_received =
      std::chrono::steady_clock::now();

  TLOG_DEBUG(5) << "Adding callbacks for each subscriber";
  for (auto subscriber : subscribers) {
    TLOG() << "subscriber ====> " << subscriber;
  }

  // Use condition variable for completion signaling
  std::mutex completion_mutex;
  std::condition_variable completion_cv;

  // Collect all written file pathnames for bookkeeping
  std::mutex pathnames_mutex;
  std::vector<std::string> written_pathnames;

  // Global counter for all received messages across all subscribers
  std::atomic<size_t> total_msgs_received{0};
  size_t total_expected =
      m_num_messages; // m_num_messages will be reset after completion

  for (auto info : subscribers) {
    // Build unique connection name for this subscriber
    std::string conn_name = "conn_A1_G" + std::to_string(info->group_id) +
                            "_C" + std::to_string(info->conn_id) + "_";

    TLOG() << "Setting up subscriber with connection: " << conn_name;

    auto recv_proc = [=, &last_received, &completion_mutex, &completion_cv,
                      &pathnames_mutex, &written_pathnames,
                      &total_msgs_received,
                      conn_name](trigger_record_ptr_t &tr) {
      m_trigger_timestamp =
          tr->get_fragments_ref().at(0)->get_trigger_timestamp();
      m_trigger_number = tr->get_fragments_ref().at(0)->get_trigger_number();
      m_run_number = tr->get_fragments_ref().at(0)->get_run_number();
      size_t file_index = get_file_index();

      TLOG() << "run_number " << m_run_number << " file index: " << file_index
             << ", trigger number: " << m_trigger_number;

      // Increment both local and global counters
      info->msgs_received++;
      size_t current_total = ++total_msgs_received;
      last_received = std::chrono::steady_clock::now();

      TLOG() << "Subscriber msgs: " << info->msgs_received
             << ", Total received: " << current_total << "/" << total_expected;

      // Write every trigger record to its own file
      std::string app_name = "test";
      std::string file_pathname_prefix = m_odir + "/" + m_output_h5_filename;

      // Generate unique filename for this trigger record
      std::string file_pathname = generate_hdf5file_pathname(
          file_pathname_prefix, m_run_number, file_index, m_trigger_number);

      TLOG() << "Writing TR " << current_total << "/" << total_expected
             << " to " << file_pathname;

      unsigned compression_level = 0;

      try {
        std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
            file_pathname, m_run_number, m_file_index, app_name, fl_pars,
            srcid_geoid_map, compression_level));

        h5file_ptr->write(*tr);
        h5file_ptr.reset();

        TLOG() << "Successfully wrote TR " << current_total
               << " with trigger_number " << m_trigger_number;

        // Store pathname for bookkeeping
        {
          std::lock_guard<std::mutex> lock(pathnames_mutex);
          written_pathnames.push_back(file_pathname);
        }

        // Send bookkeeping for this individual TR
        dunedaq::datafilter::BookKeeping bk_info("bookkeeping0");
        bk_info.entry_id =
            time_point_to_string(std::chrono::system_clock::now());
        bk_info.conn_id = m_init_connection;
        bk_info.from_id = "FilterResultWriter";
        bk_info.tr_status = "written";
        bk_info.run_number = m_run_number;
        bk_info.tr_header_info.push_back(
            {"run_number", std::to_string(m_run_number)});
        bk_info.tr_header_info.push_back(
            {"trigger_number", std::to_string(m_trigger_number)});
        bk_info.tr_header_info.push_back(
            {"trigger_timestamp", std::to_string(m_trigger_timestamp)});
        bk_info.tr_header_info.push_back({"tr_writer_pathname", file_pathname});
        bk_info.tr_header_info.push_back({"connection_name", conn_name});
        bk_info.tr_header_info.push_back(
            {"tr_count", std::to_string(current_total) + "/" +
                             std::to_string(total_expected)});

        auto bookkeeping_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
                "bookkeeping0");
        bookkeeping_sender->send(std::move(bk_info), Sender::s_block);

      } catch (const std::exception &e) {
        TLOG() << "ERROR writing TR: " << e.what();

        // Send error bookkeeping
        dunedaq::datafilter::BookKeeping bk_error("bookkeeping0");
        bk_error.entry_id =
            time_point_to_string(std::chrono::system_clock::now());
        bk_error.conn_id = m_init_connection;
        bk_error.from_id = "FilterResultWriter";
        bk_error.tr_status = "write_failed";
        bk_error.run_number = m_run_number;
        bk_error.tr_header_info.push_back(
            {"run_number", std::to_string(m_run_number)});
        bk_error.tr_header_info.push_back(
            {"trigger_number", std::to_string(m_trigger_number)});
        bk_error.tr_header_info.push_back({"error", e.what()});

        auto bookkeeping_sender =
            dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
                "bookkeeping0");
        bookkeeping_sender->send(std::move(bk_error), Sender::s_block);
      }

      // Check if all expected messages have been received
      if (current_total >= total_expected) {
        TLOG() << "All " << total_expected << " TRs received and written";
        info->complete = true;

        // Notify that a subscriber completed
        {
          std::lock_guard<std::mutex> lock(completion_mutex);
          completion_cv.notify_one();
        }
      }
    };

    m_init_connection = "conn_A1_G0_C0_";
    auto before_receiver = std::chrono::steady_clock::now();
    auto receiver = dunedaq::get_iom_receiver<
        std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>(
        m_init_connection);
    auto after_receiver = std::chrono::steady_clock::now();
    receiver->add_callback(recv_proc);
    auto after_callback = std::chrono::steady_clock::now();
    info->get_receiver_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(after_receiver -
                                                              before_receiver);
    info->add_callback_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(after_callback -
                                                              after_receiver);
  }

  // ACK to datafilter that all TRs have been written
  TLOG() << "Send final bookkeeping info to datafilter server";
  TLOG() << "Total files written: " << written_pathnames.size();
  TLOG() << "Expected: " << total_expected
         << ", Actual: " << total_msgs_received.load();

  if (written_pathnames.size() < total_expected) {
    TLOG() << "WARNING: Missing " << (total_expected - written_pathnames.size())
           << " TRs!";
  }

  dunedaq::datafilter::BookKeeping final_bk_info("bookkeeping0");
  final_bk_info.entry_id =
      time_point_to_string(std::chrono::system_clock::now());
  final_bk_info.conn_id = m_init_connection;
  final_bk_info.from_id = "FilterResultWriter";
  final_bk_info.tr_status = "all_complete";
  final_bk_info.run_number = m_run_number;
  final_bk_info.tr_header_info.push_back(
      {"run_number", std::to_string(m_run_number)});
  final_bk_info.tr_header_info.push_back(
      {"total_trs_written", std::to_string(written_pathnames.size())});
  final_bk_info.tr_header_info.push_back(
      {"expected_trs", std::to_string(total_expected)});

  // Add all file pathnames to bookkeeping
  for (size_t i = 0; i < written_pathnames.size(); ++i) {
    final_bk_info.tr_header_info.push_back(
        {"file_" + std::to_string(i), written_pathnames[i]});
  }

  auto final_bookkeeping_sender =
      dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>("bookkeeping0");
  final_bookkeeping_sender->send(std::move(final_bk_info), Sender::s_block);

  TLOG_DEBUG(5) << "Starting wait loop for receives to complete";

  // Wait for total expected messages with timeout
  {
    std::unique_lock<std::mutex> lock(completion_mutex);
    bool completed =
        completion_cv.wait_for(lock, std::chrono::seconds(60), [&]() {
          size_t current = total_msgs_received.load();
          TLOG_DEBUG(6) << "Total received: " << current
                        << ", expected: " << total_expected;
          return current >= total_expected;
        });

    if (!completed) {
      TLOG() << "WARNING: Timeout waiting for all TRs. Received "
             << total_msgs_received.load() << "/" << total_expected;
      TLOG() << "Check if sender is still sending or connection names match";
    }
  }

  TLOG_DEBUG(5) << "Removing callbacks";
  for (auto &info : subscribers) {
    std::string conn_name = "conn_A1_G" + std::to_string(info->group_id) +
                            "_C" + std::to_string(info->conn_id) + "_";
    auto receiver = dunedaq::get_iom_receiver<trigger_record_ptr_t>(conn_name);
    receiver->remove_callback();
  }

  auto cb_receiver1 =
      dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("trwriter0");
  cb_receiver1->remove_callback();

  subscribers.clear();
  TLOG_DEBUG(5) << "receive() done";
}

void FilterResultWriter::receive_tr_single_connection() {
  std::mutex cv_mutex;
  std::condition_variable cv;
  bool handshake_done = false;

  auto cb_receiver =
      dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>("trwriter0");
  std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
      [&](dunedaq::datafilter::Handshake msg) {
        if (msg.msg_id == "write_tr") {
          m_num_messages = msg.total_tr;
          std::lock_guard<std::mutex> lock(cv_mutex);
          handshake_done = true;
          cv.notify_one();
        }
        TLOG() << "FilterResultWriter: TR receiver callback: " << msg.msg_id;
      };

  cb_receiver->add_callback(str_receiver_cb);

  {
    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait_for(lock, std::chrono::seconds(10), [&] { return handshake_done; });
  }

  if (!handshake_done) {
    TLOG() << "Warning: Handshake timeout";
  }

  HDF5FileLayoutParameters fl_pars = create_file_layout_params();
  auto srcid_geoid_map = create_srcid_geoid_map();

  // Use a single connection for all TRs
  std::string single_connection = "conn_A1_G0_C0_";
  TLOG() << "Listening for ALL TRs on single connection: " << single_connection;
  TLOG() << "Expecting " << m_num_messages << " TRs total";

  std::atomic<size_t> total_msgs_received{0};
  size_t total_expected = m_num_messages;

  std::mutex completion_mutex;
  std::condition_variable completion_cv;
  std::mutex pathnames_mutex;
  std::vector<std::string> written_pathnames;

  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);

  // Single callback that processes ALL TRs
  auto recv_proc = [&, fl_pars, srcid_geoid_map](trigger_record_ptr_t &tr) {
    m_trigger_timestamp =
        tr->get_fragments_ref().at(0)->get_trigger_timestamp();
    m_trigger_number = tr->get_fragments_ref().at(0)->get_trigger_number();
    m_run_number = tr->get_fragments_ref().at(0)->get_run_number();
    size_t file_index = get_file_index();

    size_t current_total = ++total_msgs_received;

    TLOG() << "Received TR " << current_total << "/" << total_expected
           << " - run: " << m_run_number << ", trigger: " << m_trigger_number
           << ", file_index: " << file_index;

    // Write each TR to its own file
    std::string app_name = "test";
    std::string file_pathname_prefix = m_odir + "/" + m_output_h5_filename;
    std::string file_pathname = generate_hdf5file_pathname(
        file_pathname_prefix, m_run_number, file_index, m_trigger_number);

    TLOG() << "Writing TR " << current_total << "/" << total_expected << " to "
           << file_pathname;

    unsigned compression_level = 0;

    try {
      std::unique_ptr<HDF5RawDataFile> h5file_ptr(new HDF5RawDataFile(
          file_pathname, m_run_number, m_file_index, app_name, fl_pars,
          srcid_geoid_map, compression_level));

      h5file_ptr->write(*tr);
      h5file_ptr.reset();

      TLOG() << "Successfully wrote TR " << current_total
             << " with trigger_number " << m_trigger_number;

      // Store pathname
      {
        std::lock_guard<std::mutex> lock(pathnames_mutex);
        written_pathnames.push_back(file_pathname);
      }

      // Send bookkeeping
      dunedaq::datafilter::BookKeeping bk_info("bookkeeping0");
      bk_info.entry_id = time_point_to_string(std::chrono::system_clock::now());
      bk_info.conn_id = single_connection;
      bk_info.from_id = "FilterResultWriter";
      bk_info.tr_status = "written";
      bk_info.run_number = m_run_number;
      bk_info.tr_header_info.push_back(
          {"run_number", std::to_string(m_run_number)});
      bk_info.tr_header_info.push_back(
          {"trigger_number", std::to_string(m_trigger_number)});
      bk_info.tr_header_info.push_back(
          {"trigger_timestamp", std::to_string(m_trigger_timestamp)});
      bk_info.tr_header_info.push_back({"tr_writer_pathname", file_pathname});
      bk_info.tr_header_info.push_back({"connection_name", single_connection});
      bk_info.tr_header_info.push_back(
          {"tr_count", std::to_string(current_total) + "/" +
                           std::to_string(total_expected)});

      auto bookkeeping_sender =
          dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
              "bookkeeping0");
      bookkeeping_sender->send(std::move(bk_info), Sender::s_block);

    } catch (const std::exception &e) {
      TLOG() << "ERROR writing TR: " << e.what();

      dunedaq::datafilter::BookKeeping bk_error("bookkeeping0");
      bk_error.entry_id =
          time_point_to_string(std::chrono::system_clock::now());
      bk_error.conn_id = single_connection;
      bk_error.from_id = "FilterResultWriter";
      bk_error.tr_status = "write_failed";
      bk_error.run_number = m_run_number;
      bk_error.tr_header_info.push_back(
          {"run_number", std::to_string(m_run_number)});
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
      std::lock_guard<std::mutex> lock(completion_mutex);
      completion_cv.notify_one();
    }
  };

  // Set up single receiver
  auto receiver = dunedaq::get_iom_receiver<
      std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>>(
      single_connection);
  receiver->add_callback(recv_proc);

  TLOG() << "Callback registered, waiting for " << total_expected << " TRs...";

  // Wait for all TRs
  {
    std::unique_lock<std::mutex> lock(completion_mutex);
    bool completed =
        completion_cv.wait_for(lock, std::chrono::seconds(120), [&]() {
          return total_msgs_received.load() >= total_expected;
        });

    if (!completed) {
      TLOG() << "WARNING: Timeout waiting for all TRs. Received "
             << total_msgs_received.load() << "/" << total_expected;
    }
  }

  if (total_expected > 0) {
    // Send final bookkeeping
    TLOG() << "Send final bookkeeping info to datafilter server";
    TLOG() << "Total files written: " << written_pathnames.size();
    TLOG() << "Expected: " << total_expected
           << ", Actual: " << total_msgs_received.load();

    if (written_pathnames.size() < total_expected) {
      TLOG() << "WARNING: Missing "
             << (total_expected - written_pathnames.size()) << " TRs!";
    }

    dunedaq::datafilter::BookKeeping final_bk_info("bookkeeping0");
    final_bk_info.entry_id =
        time_point_to_string(std::chrono::system_clock::now());
    final_bk_info.conn_id = single_connection;
    final_bk_info.from_id = "FilterResultWriter";
    final_bk_info.tr_status = "all_complete";
    final_bk_info.run_number = m_run_number;
    final_bk_info.tr_header_info.push_back(
        {"run_number", std::to_string(m_run_number)});
    final_bk_info.tr_header_info.push_back(
        {"total_trs_written", std::to_string(written_pathnames.size())});
    final_bk_info.tr_header_info.push_back(
        {"expected_trs", std::to_string(total_expected)});

    for (size_t i = 0; i < written_pathnames.size(); ++i) {
      final_bk_info.tr_header_info.push_back(
          {"file_" + std::to_string(i), written_pathnames[i]});
    }

    auto final_bookkeeping_sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
            "bookkeeping0");
    final_bookkeeping_sender->send(std::move(final_bk_info), Sender::s_block);
  }

  // Cleanup
  receiver->remove_callback();
  cb_receiver->remove_callback();

  // reset total_expected with m_num_messsages
  m_num_messages = 0;
  TLOG_DEBUG(5) << "receive_tr_single_connection() done";
}

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
