#ifndef DATAFILTER_DATAFILTERCONNECTIONS_HPP_
#define DATAFILTER_DATAFILTERCONNECTIONS_HPP_

#include <string>
#include <vector>

namespace dunedaq::datafilter {

/**
 * @brief Extended Connections structure supporting multiple input data types.
 *
 * This structure holds all connection UIDs used by the DataFilter system.
 * It now supports both TriggerRecords and TimeSlices as input data types,
 * enabling the filter to process either or both types of data.
 *
 * Connection naming convention:
 *   - *_rx: Receiver (input to this module)
 *   - *_tx: Sender (output from this module)
 *   - *_ctrl: Control/handshake channel
 */
struct Connections {
  // ═══════════════════════════════════════════════════════════════════════════
  // Control / Request Channels (Handshake messages)
  // ═══════════════════════════════════════════════════════════════════════════

  // FilterOrchestrator control (df_ready notification)
  std::vector<std::string> fo_ctrl;

  // TRDispatcher request channels
  std::vector<std::string> trdispatcher_req_rx;  // TRDispatcher receives requests
  std::vector<std::string> trdispatcher_req_tx;  // DataFilter sends requests (via FO)

  // TSDispatcher request channels (for TimeSlice dispatcher)
  std::vector<std::string> tsdispatcher_req_rx;  // TSDispatcher receives requests
  std::vector<std::string> tsdispatcher_req_tx;  // DataFilter sends requests

  // Writer control channels
  std::vector<std::string> trwriter_ctrl;        // DataFilter -> TRWriter (notify)
  std::vector<std::string> tswriter_ctrl;        // DataFilter -> TSWriter (notify)

  // Tracking channels (for total count signaling)
  std::vector<std::string> tr_tracking_rx;       // DataFilter receives TR tracking
  std::vector<std::string> tr_tracking_tx;       // TRDispatcher sends TR tracking
  std::vector<std::string> ts_tracking_rx;       // DataFilter receives TS tracking
  std::vector<std::string> ts_tracking_tx;       // TSDispatcher sends TS tracking

  // ═══════════════════════════════════════════════════════════════════════════
  // TriggerRecord Data Channels
  // ═══════════════════════════════════════════════════════════════════════════

  std::vector<std::string> tr_data_rx;  // TR inputs (upstream → DataFilter)
  std::vector<std::string> tr_data_tx;  // TR outputs (DataFilter → downstream)

  // ═══════════════════════════════════════════════════════════════════════════
  // TimeSlice Data Channels
  // ═══════════════════════════════════════════════════════════════════════════

  std::vector<std::string> ts_data_rx;  // TimeSlice inputs (upstream → DataFilter)
  std::vector<std::string> ts_data_tx;  // TimeSlice outputs (DataFilter → downstream)

  // ═══════════════════════════════════════════════════════════════════════════
  // Bookkeeping Channels
  // ═══════════════════════════════════════════════════════════════════════════

  std::vector<std::string> bk_outputs;
  std::vector<std::string> bk_inputs;

  // ═══════════════════════════════════════════════════════════════════════════
  // Helper Methods
  // ═══════════════════════════════════════════════════════════════════════════

  /// Check if TriggerRecord inputs are configured
  bool has_tr_inputs() const { return !tr_data_rx.empty(); }

  /// Check if TimeSlice inputs are configured
  bool has_ts_inputs() const { return !ts_data_rx.empty(); }

  /// Check if any data inputs are configured
  bool has_data_inputs() const { return has_tr_inputs() || has_ts_inputs(); }

  /// Get total number of data input channels
  size_t total_data_input_count() const {
    return tr_data_rx.size() + ts_data_rx.size();
  }

  /// Check if this is a multi-input configuration
  bool is_multi_input() const { return has_tr_inputs() && has_ts_inputs(); }
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_DATAFILTERCONNECTIONS_HPP_
