/**
 * @file TRDispatcher.cpp
 *
 * Implementations of TRDispatcher's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "TRDispatcher.hpp"

namespace dunedaq::datafilter {

TRDispatcher::TRDispatcher(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_thread(std::bind(&TRDispatcher::do_work, this, std::placeholders::_1)) {
  register_command("conf", &TRDispatcher::do_conf);
  register_command("start", &TRDispatcher::do_start);
  register_command("stop", &TRDispatcher::do_stop);
}

void TRDispatcher::init(std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
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
  auto mdal = mcfg->get_dal<dunedaq::datafilter::dal::TRDispatcher>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  m_cx = dunedaq::datafilter::ConnectionsBuilder::build_from_dal(mdal);
  m_tr_tracking_tx = m_cx.tr_tracking_tx; // signal to DF
  m_tr_connections_o = m_cx.tr_data_tx;   // TriggerRecord outputs

  if (!m_cx.bk_outputs.empty())
    m_bk_connection_o = m_cx.bk_outputs.front(); // Bookkeeping out

  m_storage_pathname = mdal->get_storage_pathname();
  m_is_from_storage = mdal->get_is_from_storage();

  m_input_h5_filename = mdal->get_input_h5_filename();
  if (!m_is_from_storage)
    m_input_h5_filename = m_storage_pathname + "/" + m_input_h5_filename;

  m_json_file = mdal->get_json_file();
  m_generate_trigger_record = mdal->get_generate_trigger_record();
  if (m_generate_trigger_record)
    TLOG() << "You select to generate trigger record instead of get it from "
              "storage or a specific file!";

  m_send_timeout_ms = std::chrono::milliseconds(mdal->get_send_timeout_ms());
  m_recv_timeout_ms = std::chrono::milliseconds(mdal->get_recv_timeout_ms());

  TLOG() << "The storage for the HDF5 files is set to " << m_storage_pathname
         << " input_h5_filename " << m_input_h5_filename
         << "  m_is_from_storage " << m_is_from_storage;
}

void TRDispatcher::do_conf(const data_t &) {
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

  // use the first only, for now.
  m_trdispatcher_req_rx = m_cx.trdispatcher_req_rx.front();
  if (m_trdispatcher_req_rx.empty()) {
    TLOG() << "WARNING: No handshake receiver UID could be resolved from "
           << "ConnectionsBuilder::trdispatcher_req; falling back to legacy "
              "'trdispatcher0'.";
    m_trdispatcher_req_rx = "trdispatcher0"; // legacy fallback
  }

  // log discovered outputs
  for (auto &tr_tx : m_tr_connections_o) {
    TLOG() << "TR data TX discovered: " << tr_tx;
  }

  if (!m_bk_connection_o.empty()) {
    TLOG() << "Bookkeeping TX discovered: " << m_bk_connection_o;
  }

  TLOG() << get_name() << ": exist do_conf()";
}

void TRDispatcher::do_start(const data_t &) {
  // temporary no thread. Will be back later.
  // m_thread.start_working_thread();
  get_from_storage();
}

void TRDispatcher::get_from_storage() {

  std::vector<std::filesystem::path> files;
  size_t cnt = 0;

  TLOG() << "m_is_from_storage " << m_is_from_storage;

  if (!m_generate_trigger_record) {

    bool is_hdf5file = true;
    if (!m_is_from_storage) {
      receive(is_hdf5file);
    } else {
      while (true) {
        files = get_hdf5files_from_storage();

        if (files.size() > 0) {
          for (auto file : files) {
            m_input_h5_filename = file;
            TLOG() << "Sending from " << m_storage_pathname << "file "
                   << m_input_h5_filename;
            receive(is_hdf5file);
          }
          // Short sleep after processing files in case they come in bursts
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } else {
          // Longer sleep when no files found
          std::this_thread::sleep_for(std::chrono::milliseconds(500));
          cnt++;
          if (cnt % 120 == 0) { // Log every minute (120 * 500ms = 60s)
            TLOG() << "IDLE: No new HDF5 files after " << (cnt * 500 / 1000)
                   << " seconds.";
          }
        }
      }
    }
  } else {
    bool is_hdf5file = false;
    receive(is_hdf5file);
  }
}

void TRDispatcher::do_stop(const data_t &) {

  // m_thread.stop_working_thread();
}

void TRDispatcher::do_work(std::atomic<bool> &running_flag) {

  std::mutex work_mutex;
  std::condition_variable work_cv;
  std::vector<std::filesystem::path> files;
  size_t cnt = 0;

  TLOG() << "m_is_from_storage " << m_is_from_storage;
  while (running_flag.load()) {
    get_from_storage();

    std::unique_lock<std::mutex> lock(work_mutex);
    work_cv.wait_for(lock, std::chrono::seconds(1), [&]() {
      return !running_flag.load(); // check for new do_work availability
    });
  }
}

void TRDispatcher::generate_opmon_data() {
  dunedaq::datafilter::opmon::TRDispatcherInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

// Receive handshake from FilterOrchestrator
void TRDispatcher::receive(bool is_hdf5file) {
  bool handshake_done = false;
  std::atomic<unsigned int> received_cnt = 0;

  auto cb_receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
      m_trdispatcher_req_rx);

  std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
      [&](dunedaq::datafilter::Handshake msg) {
        if (msg.msg_id == m_trdispatcher_req_rx) {
          ++received_cnt;
        }
        TLOG() << "Received next TR instruction from filter "
                  "orchestrator: "
               << msg.msg_id;
      };

  cb_receiver->add_callback(str_receiver_cb);
  while (!handshake_done) {
    if (received_cnt == 1)
      handshake_done = true;
  }

  cb_receiver->remove_callback();

  if (is_hdf5file) {
    send_tr_from_hdf5file();
    send_ts_from_hdf5file();
  } else {
    send_tr();
  }
}

// generate a dummy test trigger record to be send to datafilter
trigger_record_ptr_t TRDispatcher::create_trigger_record(uint64_t trig_num) {
  std::vector<char> dummy_vector(fragment_size);

  for (auto &i : dummy_vector) {
    i = std::rand();
  }
  char *dummy_data = dummy_vector.data();

  // generate the timestamp for trigger record
  int64_t ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                   system_clock::now().time_since_epoch())
                   .count();

  // create TriggerRecordHeader
  dunedaq::daqdataformats::TriggerRecordHeaderData trh_data;
  trh_data.trigger_number = trig_num;
  trh_data.trigger_timestamp = ts;
  trh_data.num_requested_components = components_per_record;
  trh_data.run_number = run_number;
  trh_data.sequence_number = 0;
  trh_data.max_sequence_number = 1;
  trh_data.element_id = dunedaq::daqdataformats::SourceID(
      dunedaq::daqdataformats::SourceID::Subsystem::kTRBuilder, 0);

  dunedaq::daqdataformats::TriggerRecordHeader trh(&trh_data);

  // create our TriggerRecord
  auto tr = std::make_unique<dunedaq::daqdataformats::TriggerRecord>(trh);

  // loop over elements tpc
  for (size_t ele_num = 0; ele_num < element_count_tpc; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kWIB);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kHD_TPC);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
        ele_num);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  // loop over elements pds
  for (size_t ele_num = 0; ele_num < element_count_pds; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kDAPHNE);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kHD_PDS);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kDetectorReadout,
        ele_num + element_count_tpc);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    // tr.add_fragment(std::move(frag_ptr));
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  // loop over TriggerActivity
  for (size_t ele_num = 0; ele_num < element_count_ta; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts - 10;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kTriggerActivity);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kDAQ);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kTrigger, ele_num);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  // loop over TriggerCandidate
  for (size_t ele_num = 0; ele_num < element_count_tc; ++ele_num) {
    // create our fragment
    dunedaq::daqdataformats::FragmentHeader fh;
    fh.trigger_number = trig_num;
    fh.trigger_timestamp = ts;
    fh.window_begin = ts;
    fh.window_end = ts;
    fh.run_number = run_number;
    fh.fragment_type = static_cast<dunedaq::daqdataformats::fragment_type_t>(
        dunedaq::daqdataformats::FragmentType::kTriggerCandidate);
    fh.sequence_number = 0;
    fh.detector_id = static_cast<uint16_t>(
        dunedaq::detdataformats::DetID::Subdetector::kDAQ);
    fh.element_id = dunedaq::daqdataformats::SourceID(
        dunedaq::daqdataformats::SourceID::Subsystem::kTrigger,
        ele_num + element_count_ta);

    std::unique_ptr<dunedaq::daqdataformats::Fragment> frag_ptr(
        new dunedaq::daqdataformats::Fragment(dummy_data, fragment_size));
    frag_ptr->set_header_fields(fh);

    // add fragment to TriggerRecord
    tr->add_fragment(std::move(frag_ptr));

  } // end loop over elements

  trigger_record_ptr_t temp = std::move(tr);
  return temp;
}

// send trigger records from self generated TR
void TRDispatcher::send_tr() {
  std::ostringstream ss;
  auto trig_num = 9999; // fake trigger number for generating TR

  // m_trdispatcher_id = "conn_A0_G0_C0_"; // to get it from config.
  m_trdispatcher_id = m_cx.tr_data_tx.front();

  if (m_cx.tr_data_tx.empty()) {
    TLOG() << "No tr_data_tx discovered; skipping TR send.";
    return;
  }

  if (m_cx.tr_tracking_tx.empty()) {
    TLOG() << "No tr_tracking_tx discovered; Making sure that tracking is in "
              "the OKS file.";
    return;
  }
  auto init_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
      m_cx.tr_tracking_tx.front());

  dunedaq::datafilter::Handshake sent_t1("next_tr");
  init_sender->send(std::move(sent_t1), Sender::s_block);

  std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
  std::mutex tracking_mutex;

  auto info = std::make_shared<TRDispatcherInfo>(0, 0);
  trdispatchers.push_back(info);

  TLOG_DEBUG(7) << "Getting publisher objects for each connection";
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers), [=](std::shared_ptr<TRDispatcherInfo> info) {
        auto before_sender = std::chrono::steady_clock::now();

        info->sender =
            dunedaq::get_iom_sender<trigger_record_ptr_t>(m_trdispatcher_id);
        auto after_sender = std::chrono::steady_clock::now();
        info->get_sender_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_sender - before_sender);
      });

  TLOG_DEBUG(7) << "Starting publish threads";
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers),
      [=, &completed_receiver_tracking,
       &tracking_mutex](std::shared_ptr<TRDispatcherInfo> info) {
        info->send_thread.reset(new std::thread(
            [=, &completed_receiver_tracking, &tracking_mutex]() {
              bool complete_received = false;

              std::this_thread::sleep_for(100ms);
              while (!complete_received) {
                TLOG() << "Sender message: generate trigger "
                          "record";
                trigger_record_ptr_t temp_record(
                    create_trigger_record(trig_num));

                TLOG() << "Start sending  trigger record";
                info->sender->try_send(
                    std::move(temp_record),
                    std::chrono::milliseconds(m_send_timeout_ms));
                TLOG() << "End sending trigger record";
                ++info->messages_sent;
                {
                  std::lock_guard<std::mutex> lk(tracking_mutex);
                  if ((completed_receiver_tracking.count(info->group_id) &&
                       completed_receiver_tracking[info->group_id].count(
                           info->conn_id)) ||
                      completed_receiver_tracking.count(-1)) {
                    TLOG() << "Complete_received";
                    complete_received = true;
                  }
                }
                complete_received = true;
                break;
              } // while loop
            }));
      });

  TLOG_DEBUG(7) << "Joining send threads";
  for (auto &sender : trdispatchers) {
    sender->send_thread->join();
    sender->send_thread.reset(nullptr);
  }
  trdispatchers.clear();
  TLOG() << "TR send done; it will start the next send.";
}

// Send trigger records from generated hdf5 files.
void TRDispatcher::send_tr_from_hdf5file() {
  // Previous threads were already joined at end of last call; clear the vector
  // so this call spawns exactly ONE send thread instead of N on the N-th cycle.
  trdispatchers.clear();

  std::ostringstream oss;

  // m_trdispatcher_id = "conn_A0_G0_C0_"; // to get it from config.

  if (!m_cx.tr_data_tx.empty()) {
    m_trdispatcher_id = m_cx.tr_data_tx.front();
  } else {
    throw std::runtime_error(
        "No TriggerRecord TX connection discovered (tr_data_tx is empty)");
  }

  HDF5RawDataFile h5_file(m_input_h5_filename);
  if (!h5_file.is_trigger_record_type()) {
    TLOG_DEBUG(7) << "File " << m_input_h5_filename
                  << " is not a TriggerRecord file (record_type="
                  << h5_file.get_record_type() << "); skipping TR send.";
    return;
  }
  auto records = h5_file.get_all_trigger_record_ids();
  if (records.empty()) {
    TLOG() << "No TriggerRecords in " << m_input_h5_filename;
    return;
  }
  auto records_size = records.size();
  auto total_tr = *(std::next(records.begin(), records.size() - 1));
  oss << "Last trigger record: " << int(total_tr.first) << ","
      << total_tr.second << "\n";

  TLOG() << oss.str();
  oss.str("");
  dunedaq::datafilter::time_point_to_string time_point_to_string(
      dunedaq::datafilter::Precision::NANOSECONDS);

  auto t1 = std::chrono::system_clock::now();
  dunedaq::datafilter::BookKeeping bk_info(m_bk_connection_o);
  bk_info.entry_id = time_point_to_string(t1);
  bk_info.conn_id = m_bk_info_id;
  bk_info.from_id = "trdispatcher";
  dunedaq::datafilter::node_info node_info;
  bk_info.node = node_info.get_node_info();

  bk_info.tr_header_info.push_back(
      {"record size", std::to_string(records.size())});
  auto file_index = h5_file.get_attribute<size_t>("file_index");
  TLOG() << "File index :" << file_index;
  bk_info.run_number = h5_file.get_attribute<size_t>("run_number");
  bk_info.file_attributes_info.push_back(
      {"file_index", std::to_string(file_index)});
  bk_info.file_send_list.push_back(m_input_h5_filename);
  // Mark state: TRD is about to dispatch TRs from this file.
  bk_info.tr_status = to_string(TRStatus::kAssignedToFilter);

  // Send the file attributes first: file_index, run_number. The
  // FilterResultWriter needs to know it before receiving the trigger
  // record.

  if (m_bk_connection_o.empty()) {
    throw std::runtime_error(
        "No bookkeeping TX connection discovered (bk_outputs is empty)");
  }
  auto bookkeeping_sender =
      dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
          m_bk_connection_o);

  bookkeeping_sender->send(std::move(bk_info), Sender::s_no_block);

  if (m_cx.tr_tracking_tx.empty()) {
    TLOG() << "TR_tracking2 to DF is empty.";
    return;
  }
  TLOG() << "m_cx.tr_tracking_tx " << m_cx.tr_tracking_tx.front();
  // Handshake with datafilter.
  auto init_sender = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(
      m_cx.tr_tracking_tx.front());

  dunedaq::datafilter::Handshake sent_t1("next_tr");
  // send total trigger number to datafilter then datafilter to
  // FilterResultWriter
  sent_t1.total_tr = int(records_size);

  init_sender->send(std::move(sent_t1), Sender::s_no_block);

  std::unordered_map<int, std::set<size_t>> completed_receiver_tracking;
  std::mutex tracking_mutex;

  // for (size_t group = 0; group < config.num_groups; ++group) {
  //     for (size_t conn = 0; conn < config.num_connections_per_group;
  //          ++conn) {

  // auto info = std::make_shared<TRDispatcherInfo>(group, conn);
  auto info = std::make_shared<TRDispatcherInfo>(0, 0);
  trdispatchers.push_back(info);
  //  }
  // }

  TLOG_DEBUG(7) << "Getting publisher objects for each connection";
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers), [=](std::shared_ptr<TRDispatcherInfo> info) {
        auto before_sender = std::chrono::steady_clock::now();
        info->sender =
            dunedaq::get_iom_sender<trigger_record_ptr_t>(m_trdispatcher_id);
        auto after_sender = std::chrono::steady_clock::now();
        info->get_sender_time =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                after_sender - before_sender);
      });

  TLOG_DEBUG(7) << "Starting publish threads";
  std::for_each(
      std::execution::par_unseq, std::begin(trdispatchers),
      std::end(trdispatchers),
      [=, &bk_info, &h5_file, &completed_receiver_tracking,
       &tracking_mutex](std::shared_ptr<TRDispatcherInfo> info) {
        info->send_thread.reset(new std::thread([=, &bk_info, &h5_file,
                                                 &completed_receiver_tracking,
                                                 &tracking_mutex]() {
          bool complete_received = false;
          bool all_sends_ok = true;

          std::ostringstream oss;
          std::this_thread::sleep_for(100ms);
          while (!complete_received) {
            TLOG() << "Sender message: trigger record";

            auto records = h5_file.get_all_trigger_record_ids();
            oss << "\nNumber of TriggerRecords: " << records.size();
            if (records.empty()) {
              oss << "\n\nNO TRIGGER RECORDS FOUND";
              TLOG() << oss.str();
              break;
            }
            auto first_rec = *(records.begin());
            auto last_rec = *(std::next(records.begin(), records.size() - 1));

            oss << "\n\tFirst trigger record: " << first_rec.first << ","
                << first_rec.second;
            oss << "\n\tLast trigger record: " << last_rec.first << ","
                << last_rec.second;

            TLOG() << oss.str();
            oss.str("");

            for (auto const &rid : records) {
              try {
                auto tr = h5_file.get_trigger_record(rid);

                if (tr.get_fragments_ref().empty()) {
                  TLOG() << "TR " << rid.first << "," << rid.second
                         << " has no fragments, skipping.";
                  all_sends_ok = false;
                  continue;
                }

                m_trigger_number =
                    tr.get_fragments_ref().at(0)->get_trigger_number();
                m_run_number = tr.get_fragments_ref().at(0)->get_run_number();
                TLOG() << "Trigger number " << m_trigger_number << " run_number "
                       << m_run_number;
                // SERIALIZE
                auto bytes = dunedaq::serialization::serialize(
                    tr, dunedaq::serialization::kMsgPack);
                // DESERIALIZE
                auto deserialized =
                    dunedaq::serialization::deserialize<trigger_record_ptr_t>(
                        bytes);

                try {
                  info->sender->try_send(std::move(deserialized),
                                         std::chrono::milliseconds(50));
                } catch (const std::exception& e) {
                  TLOG() << "try_send failed for trigger record "
                         << rid.first << "," << rid.second
                         << ": " << e.what()
                         << " — will not mark source file as transferred.";
                  all_sends_ok = false;
                }
              } catch (const std::exception& e) {
                TLOG() << "get_trigger_record failed for rid "
                       << rid.first << "," << rid.second
                       << ": " << e.what()
                       << " — skipping this TR, will not mark file transferred.";
                all_sends_ok = false;
              }
            }

            ++info->messages_sent;
            {
              std::lock_guard<std::mutex> lk(tracking_mutex);
              if ((completed_receiver_tracking.count(info->group_id) &&
                   completed_receiver_tracking[info->group_id].count(
                       info->conn_id)) ||
                  completed_receiver_tracking.count(-1)) {
                TLOG() << "Complete_received";
                complete_received = true;
              }
            }
            complete_received = true;
            break;
          } // while loop

          // Gate WriteJSON on FRW write-result confirmation (bookkeeping2).
          // If bookkeeping2 is not configured, fall back to send-success flag.
          bool write_confirmed = false;

          if (!m_cx.bk_inputs.empty()) {
            auto bk_receiver =
                dunedaq::get_iom_receiver<dunedaq::datafilter::BookKeeping>(
                    m_cx.bk_inputs.front());

            std::mutex conf_mutex;
            std::string received_status;
            std::atomic<bool> got_reply{false};

            std::function<void(dunedaq::datafilter::BookKeeping)> conf_cb =
                [&](dunedaq::datafilter::BookKeeping bk) {
                  if (bk.from_id == "FilterResultWriter") {
                    std::lock_guard<std::mutex> lk(conf_mutex);
                    received_status = bk.tr_status;
                    got_reply.store(true);
                  }
                };

            bk_receiver->add_callback(conf_cb);

            // Wait up to 300 s for FRW to finish writing.
            // FRW loops through receive_tr_single_connection() calls; the first
            // call that actually handles the file can take up to ~2 min before
            // sending its final BK, so 120 s was too tight.
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(300);
            while (!got_reply.load() &&
                   std::chrono::steady_clock::now() < deadline) {
              std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }

            bk_receiver->remove_callback();

            std::string status_copy;
            {
              std::lock_guard<std::mutex> lk(conf_mutex);
              status_copy = received_status;
            }

            if (got_reply.load() &&
                status_copy ==
                    dunedaq::datafilter::to_string(TRStatus::kReRecorded)) {
              write_confirmed = true;
            } else {
              TLOG() << "TRD: FRW confirmation for " << m_input_h5_filename
                     << " — status='" << status_copy
                     << "' timeout=" << !got_reply.load()
                     << " — WriteJSON skipped; file will be retried.";
            }
          } else {
            // No bookkeeping2 configured: fall back to whether sends succeeded
            write_confirmed = all_sends_ok;
            TLOG() << "TRD: no bk_inputs (bookkeeping2) configured; using"
                      " send-success as WriteJSON gate.";
          }

          if (write_confirmed) {
            dunedaq::datafilter::HDF5FromStorage s(m_storage_pathname,
                                                   m_json_file);
            s.WriteJSON(m_input_h5_filename);
          } else {
            TLOG() << "WriteJSON skipped for " << m_input_h5_filename
                   << " — will retry on next scan cycle.";
          }

          // send book keeping info after the TR is tranfered.
          TLOG() << "Send bookkeeping info to datafilter server";
          bk_info.file_send_status = "send";
          // Reflect actual write outcome: kReRecorded if FRW confirmed all
          // TRs written, kWriteFailed if storage was full or timeout.
          bk_info.tr_status = write_confirmed
                                  ? to_string(TRStatus::kReRecorded)
                                  : to_string(TRStatus::kWriteFailed);
          bk_info.run_number = m_run_number;
          TLOG() << "run number from trdispatcher " << m_run_number;
          bk_info.tr_header_info.push_back(
              {"run number", std::to_string(m_run_number)});
          bk_info.tr_header_info.push_back(
              {"trigger number", std::to_string(m_trigger_number)});
          // Assign rather than push_back: the initial s_no_block send does not
          // consume the move, so file_send_list may already contain the entry.
          bk_info.file_send_list = {m_input_h5_filename};

          auto bookkeeping_sender =
              dunedaq::get_iom_sender<dunedaq::datafilter::BookKeeping>(
                  m_bk_connection_o);
          //// SERIALIZE
          // auto bk_bytes = dunedaq::serialization::serialize(
          //     bk_info, dunedaq::serialization::kJSON);
          //// DESERIALIZE
          // auto bk_deserialized =
          //     dunedaq::serialization::deserialize<
          //         dunedaq::datafilter::BookKeeping_json>(
          //         bk_bytes);

          bookkeeping_sender->send(std::move(bk_info), Sender::s_block);
        }));
      });

  TLOG() << "Joining send threads";
  for (auto &sender : trdispatchers) {
    sender->send_thread->join();
    sender->send_thread.reset(nullptr);
  }
}

// Send TimeSlices from the same HDF5 file.
void TRDispatcher::send_ts_from_hdf5file() {
  if (m_cx.ts_data_tx.empty()) {
    TLOG_DEBUG(7)
        << "No TimeSlice TX connections configured; skipping TS send.";
    return;
  }
  m_tsdispatcher_id = m_cx.ts_data_tx.front();

  HDF5RawDataFile h5_file(m_input_h5_filename);
  if (!h5_file.is_timeslice_type()) {
    TLOG_DEBUG(7) << "File " << m_input_h5_filename
                  << " is not a TimeSlice file (record_type="
                  << h5_file.get_record_type() << "); skipping TS send.";
    return;
  }
  auto ts_records = h5_file.get_all_timeslice_ids();
  if (ts_records.empty()) {
    TLOG_DEBUG(7) << "No TimeSlices in " << m_input_h5_filename;
    return;
  }

  TLOG() << "Sending " << ts_records.size() << " TimeSlice(s) from "
         << m_input_h5_filename;

  auto ts_sender = dunedaq::get_iom_sender<timeslice_ptr_t>(m_tsdispatcher_id);

  for (const auto &rid : ts_records) {
    auto ts = h5_file.get_timeslice(rid);
    TLOG() << "TimeSlice number " << rid.first << " sequence " << rid.second;

    auto bytes =
        dunedaq::serialization::serialize(ts, dunedaq::serialization::kMsgPack);
    auto deserialized =
        dunedaq::serialization::deserialize<timeslice_ptr_t>(bytes);

    ts_sender->try_send(std::move(deserialized),
                        std::chrono::milliseconds(m_send_timeout_ms));
  }

  TLOG() << "TimeSlice send done for " << m_input_h5_filename;
}

std::vector<std::filesystem::path> TRDispatcher::get_hdf5files_from_storage() {
  TLOG_DEBUG(7) << "I am in get_hdf5files_from_storage : storage_pathname"
                << m_storage_pathname << " json_file " << m_json_file;

  dunedaq::datafilter::HDF5FromStorage s(m_storage_pathname, m_json_file);
  // s.print();

  // for (auto file : s.hdf5_files_to_transfer) {
  //     std::cout << "main: files to transfer" << file << "\n";
  // }
  return s.hdf5_files_to_transfer;
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::TRDispatcher)
