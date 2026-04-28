/**
 * @file FilterResultWriter.hpp
 *
 * Developer(s) of this DAQModule have yet to replace this line with a brief
 * description of the DAQModule.
 *
 * This is part of the DUNE DAQ Software Suite, copyright 2020.
 * Licensing/copyright details are in the COPYING file that you should have
 * received with this code.
 */

#ifndef DFBACKEND_PLUGINS_FILTERRESULTWRITER_HPP_
#define DFBACKEND_PLUGINS_FILTERRESULTWRITER_HPP_

#include "appfwk/DAQModule.hpp"

#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"

#include "daqdataformats/TimeSlice.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "daqdataformats/TriggerRecordHeaderData.hpp"
#include "datafilter/TimeSlice_serialization.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/core/ConnectionsBuilder.hpp"
#include "datafilter/dal/FilterResultWriter.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "datafilter/opmon/filterresultwriter_info.pb.h"
#include "dfmessages/TriggerRecord_serialization.hpp"
#include "hdf5libs/HDF5RawDataFile.hpp"
#include "hdf5libs/test/HDF5TestUtils.hpp"
#include "opmonlib/TestOpMonManager.hpp"
// #include "serialization/Serialization.hpp"
#include "utilities/WorkerThread.hpp"

#include <atomic>
#include <condition_variable>
#include <execution>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <string>

using namespace dunedaq::iomanager;
using namespace dunedaq::hdf5libs;
using dataobj_t = nlohmann::json;
using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;
using timeslice_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TimeSlice>;

namespace dunedaq::datafilter {

class FilterResultWriter : public dunedaq::appfwk::DAQModule {
public:
  struct SubscriberInfo {
    size_t group_id;
    size_t conn_id;
    bool is_group_subscriber;
    std::unordered_map<size_t, size_t> last_sequence_received{0};
    std::atomic<size_t> msgs_received{0};
    std::atomic<size_t> msgs_with_error{0};
    std::chrono::milliseconds get_receiver_time;
    std::chrono::milliseconds add_callback_time;
    std::atomic<bool> complete{false};

    SubscriberInfo(size_t group, size_t conn)
        : group_id(group), conn_id(conn), is_group_subscriber(false) {}
    SubscriberInfo(size_t group)
        : group_id(group), conn_id(0), is_group_subscriber(true) {}
  };

  explicit FilterResultWriter(const std::string &name);

  void init(std::shared_ptr<appfwk::ConfigurationManager>) override;

  void set_file_index(uint32_t file_index) {
    m_file_index.store(file_index, std::memory_order_release);
  }
  size_t get_file_index() const {
    return m_file_index.load(std::memory_order_acquire);
  }
  std::string generate_hdf5file_pathname(std::string file_pathname_prefix,
                                         int run_number, int file_index,
                                         int trigger_number);
  void receive_tr();
  void receive_tr_single_connection();
  void receive_ts_single_connection();
  void send_next_tr();
  void receive_attrs_test();
  void start_receive_attrs_test_thread();
  void stop_receive_attrs_test_thread();

  std::vector<std::shared_ptr<SubscriberInfo>> subscribers;
  FilterResultWriter(const FilterResultWriter &) = delete;
  FilterResultWriter &operator=(const FilterResultWriter &) = delete;
  FilterResultWriter(FilterResultWriter &&) = delete;
  FilterResultWriter &operator=(FilterResultWriter &&) = delete;

  ~FilterResultWriter() = default;

protected:
  void generate_opmon_data() override;

private:
  // Commands FilterResultWriter can receive

  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER READING
  // IT For any run control command it is possible for a DAQModule to register
  // an action that will be executed upon reception of the command. do_conf is a
  // very common example of this; in FilterResultWriter.cpp you would implement
  // do_conf so that members of FilterResultWriter get assigned values from a
  // configuration passed as an argument and originating from the CCM system.

  void do_conf(const data_t &);
  void do_start(const data_t &);
  void do_stop(const data_t &);
  void do_work(std::atomic<bool> &running);
  void receive_attrs(std::atomic<bool> &running);

  // Threading
  dunedaq::utilities::WorkerThread m_thread;
  dunedaq::utilities::WorkerThread m_bk_thread;

  std::shared_ptr<dunedaq::conffwk::Configuration> m_confdb;
  std::vector<const dunedaq::confmodel::Queue *> m_queues;
  std::vector<const confmodel::NetworkConnection *> m_networkconnections;
  Connections m_cx;

  // TO dfbackend DEVELOPERS: PLEASE DELETE THIS FOLLOWING COMMENT AFTER READING
  // IT m_total_amount and m_amount_since_last_get_info_call are examples of
  // variables whose values get reported to OpMon
  // (https://github.com/mozilla/opmon) each time get_info() is
  // called. "amount" represents a (discrete) value which changes as
  // FilterResultWriter runs and whose value we'd like to keep track of during
  // running; obviously you'd want to replace this "in real life"

  // Configuration
  std::shared_ptr<appfwk::ConfigurationManager> m_mcfg;

  std::string m_oksConfig = "oksconflibs:test/config/dfSession.data.xml";
  std::string m_session_name = "test-session";
  size_t m_trigger_timestamp;
  size_t m_trigger_number;
  size_t m_run_number;
  std::atomic<size_t> m_num_messages{0};
  std::string m_info_file_base = "FilterResultWriter";
  std::string m_odir;
  std::string m_output_h5_filename;
  std::uintmax_t m_min_free_bytes{2ULL * 1024 * 1024 * 1024};
  // std::string m_session_name = "FilterResultWriter test run";
  std::string m_ofile_pathname{};

  std::string m_init_connection;
  std::atomic<int> m_num_groups{1};
  std::atomic<int> m_num_connections_per_group{5};
  std::atomic<size_t> m_file_index{0};

  std::atomic<int64_t> m_total_amount{0};
  std::atomic<int> m_amount_since_last_call{0};

  // Gate: do_start() waits here until DF signals a new dispatch via bookkeeping1.
  // Prevents receive_tr_single_connection() from looping and sending repeated
  // kFileCompleted messages when there is no active pipeline cycle.
  std::atomic<bool> m_dispatch_ready{false};
  std::mutex m_dispatch_mutex;
  std::condition_variable m_dispatch_cv;

  // for testing only, not used and to be removed.
  std::thread m_attrs_test_thread;
  std::atomic<bool> m_attrs_test_running{false};
  std::mutex m_attrs_test_mtx;
  std::condition_variable m_attrs_test_cv;
};

} // namespace dunedaq::datafilter

#endif // DFBACKEND_PLUGINS_FILTERRESULTWRITER_HPP_
