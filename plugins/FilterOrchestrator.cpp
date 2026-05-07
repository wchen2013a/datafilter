/**
 * @file FilterOrchestrator.cpp
 *
 * Implementations of FilterOrchestrator's functions
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#include "FilterOrchestrator.hpp"

namespace dunedaq::datafilter {

FilterOrchestrator::FilterOrchestrator(const std::string &name)
    : dunedaq::appfwk::DAQModule(name),
      m_thread(std::bind(&FilterOrchestrator::do_work, this,
                         std::placeholders::_1)) {
  register_command("conf", &FilterOrchestrator::do_conf);
  register_command("start", &FilterOrchestrator::do_start);
  register_command("stop", &FilterOrchestrator::do_stop);
}

void FilterOrchestrator::init(
    std::shared_ptr<appfwk::ConfigurationManager> mcfg) {
  TLOG() << "Module name: " << get_name();

  // m_mcfg = mcfg;

  try {
    m_confdb = std::make_shared<dunedaq::conffwk::Configuration>(m_oksConfig);
  } catch (conffwk::Generic &exc) {
    std::cout << "Failed to load OKS database: " << exc << std::endl;
  }

  m_confdb->get<dunedaq::confmodel::Queue>(m_queues);
  m_confdb->get<dunedaq::confmodel::NetworkConnection>(m_networkconnections);

  // get FilterOrchestractor attributes.
  auto mdal =
      mcfg->get_dal<dunedaq::datafilter::dal::FilterOrchestrator>(get_name());

  if (mdal == nullptr) {
    throw appfwk::CommandFailed(ERS_HERE, get_name(), "init",
                                "Unable to load module configuration");
  }

  m_cx = dunedaq::datafilter::ConnectionsBuilder::build_from_dal(mdal);
  TLOG() << "FilterOrchestractor: trdispatcher_req_tx size="
         << m_cx.trdispatcher_req_tx.size()
         << " tr_tracking_rx size=" << m_cx.tr_tracking_rx.size();
}

void FilterOrchestrator::do_conf(const data_t &) {

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

void FilterOrchestrator::do_start(const data_t &) {
  TLOG() << get_name() << ": do_start()";

  while (true) {
    receive();
  }
  // m_thread.start_working_thread();
}

void FilterOrchestrator::do_stop(const data_t &) {
  TLOG() << get_name() << ": do_stop()";
  // m_thread.stop_working_thread();
}

void FilterOrchestrator::do_work(std::atomic<bool> &running_flag) {
  std::mutex work_mutex;
  std::condition_variable work_cv;
  m_running_flag = &running_flag;

  TLOG() << get_name() << ": do_work() - Starting";

  while (running_flag.load()) {
    TLOG() << "do_work() - Calling receive(), running_flag="
           << running_flag.load();
    receive();
    TLOG() << "do_work() - Returned from receive(), running_flag="
           << running_flag.load();

    std::unique_lock<std::mutex> lock(work_mutex);
    work_cv.wait_for(lock, std::chrono::seconds(1),
                     [&]() { return !running_flag.load(); });
  }

  TLOG() << get_name() << ": do_work() - Exited loop";
}

void FilterOrchestrator::request_next_tr() {
  if (m_cx.trdispatcher_req_tx.empty()) {
    TLOG() << "trdispatcher_req_tx endpoints is empty";
    return;
  }
  for (const auto &uid : m_cx.trdispatcher_req_tx) {
    try {
      auto s = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(uid);
      dunedaq::datafilter::Handshake send_t1("trdispatcher0");
      s->send(std::move(send_t1), Sender::s_block);
      TLOG() << "Sent request_next_tr TRDispatcher - Sucess to " << uid;
    } catch (const std::exception &e) {
      TLOG() << "Sent request_next_tr TRDispatcher - Failed: " << e.what();
    }
  }

  TLOG() << "Sent request_next_tr TRDispatcher - Exiting";
}

void FilterOrchestrator::receive() {
  std::mutex cv_mutex;
  std::condition_variable cv;
  TLOG() << "receive() - Starting";
  bool handshake_done = false;

  TLOG() << "m_cx.trdispatcher_req_rx ====> "
         << m_cx.trdispatcher_req_rx.front();

  auto cb_receiver = dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(
      m_cx.trdispatcher_req_rx.front());

  std::function<void(dunedaq::datafilter::Handshake)> str_receiver_cb =
      [&](dunedaq::datafilter::Handshake msg) {
        TLOG() << "receive() - Callback fired! msg_id: " << msg.msg_id;
        if (msg.msg_id == "next_tr" || msg.msg_id == "next_ts") {
          std::lock_guard<std::mutex> lock(cv_mutex);
          handshake_done = true;
          TLOG() << "Received " << msg.msg_id << " instruction from Data Filter";
        }
      };

  cb_receiver->add_callback(str_receiver_cb);

  {
    std::unique_lock<std::mutex> lock(cv_mutex);
    cv.wait_for(lock, std::chrono::seconds(30), [&] { return handshake_done; });
  }

  TLOG() << "receive() - Waiting for message...";

  auto start = std::chrono::steady_clock::now();
  while (!handshake_done && (std::chrono::steady_clock::now() - start) <
                                std::chrono::seconds(10)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  TLOG() << "receive() - Exiting function";
  cb_receiver->remove_callback();
  request_next_tr();
}

void FilterOrchestrator::generate_opmon_data() {
  dunedaq::datafilter::opmon::FilterOrchestratorInfo info;
  info.set_total_amount(m_total_amount.load());
  info.set_amount_since_last_call(m_amount_since_last_call.exchange(0));
  publish(std::move(info));
}

} // namespace dunedaq::datafilter

DEFINE_DUNE_DAQ_MODULE(dunedaq::datafilter::FilterOrchestrator)
