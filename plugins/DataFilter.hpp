/**
 * @file DataFilter.hpp
 *
 * Developer(s) of this DAQModule have yet to replace this line with a brief
 * description of the DAQModule.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef DATAFILTER_PLUGINS_DATAFILTER_HPP_
#define DATAFILTER_PLUGINS_DATAFILTER_HPP_

#include "appfwk/DAQModule.hpp"
#include "opmonlib/TestOpMonManager.hpp"
#include "utilities/WorkerThread.hpp"

#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"

#include "datafilter/bookkeeping_manager.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/core/ConnectionsBuilder.hpp"
#include "datafilter/core/DataFilterOrganiser.hpp"
#include "datafilter/core/DataFilterReceiver.hpp"
#include "datafilter/core/DataFilterTRSink.hpp"

#include "datafilter/dal/DataFilter.hpp"
#include "datafilter/opmon/datafilter_info.pb.h"

#include <memory>

namespace dunedaq::datafilter {

class DataFilter : public dunedaq::appfwk::DAQModule {
public:
  explicit DataFilter(const std::string &name);
  void init(std::shared_ptr<appfwk::ConfigurationManager>) override;

  ~DataFilter() {
    if (m_bk) {
      m_bk->stop();
    }
  }

protected:
  void generate_opmon_data() override;

private:
  using data_t = nlohmann::json;
  void do_conf(const data_t &);
  void do_start(const data_t &);
  void do_stop(const data_t &);
  void do_work(std::atomic<bool> &running_flag);

  void print_attrs();

  dunedaq::datafilter::RunInfo m_run_info{};
  std::string m_datafilter_id;

  // Wiring
  Connections m_connections;
  std::shared_ptr<TRRewriterSink> m_sink;
  std::shared_ptr<DataFilterOrganiser> m_organiser;
  std::unique_ptr<DataFilterReceiver> m_rx;
  std::shared_ptr<dunedaq::datafilter::BookkeepingReceiver> m_bk;

  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;
  std::shared_ptr<dunedaq::conffwk::Configuration> m_confdb;

  std::vector<std::string> m_tr_connections_o;
  std::string m_bk_connection_o;

  dunedaq::utilities::WorkerThread m_thread;
  // TO datafilter DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER
  // READING IT m_total_amount and m_amount_since_last_get_info_call are
  // examples of variables whose values get reported to OpMon
  // (https://github.com/mozilla/opmon) each time get_info() is
  // called. "amount" represents a (discrete) value which changes as
  // DataFilter runs and whose value we'd like to keep track of during
  // running; obviously you'd want to replace this "in real life"

  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";
  std::shared_ptr<appfwk::ConfigurationManager> m_mcfg;

  std::string address;
  std::string data_type;
  std::string conn_type_str;

  std::string m_session_name = "test-session";

  std::atomic<int64_t> m_total_amount{0};
  std::atomic<int> m_amount_since_last_call{0};
};

} // namespace dunedaq::datafilter
#endif // DATAFILTER_PLUGINS_DATAFILTER_HPP_
