#ifndef DATAFILTER_DATAFILTERCONNECTIONS_HPP_
#define DATAFILTER_DATAFILTERCONNECTIONS_HPP_

#include <string>
#include <vector>

namespace dunedaq::datafilter {

struct Connections {
  // Control/request lanes
  std::vector<std::string> fo_ctrl; // DataFilter notify FilterOrchestrator
                                    // (df_ready), this is not used.
  std::vector<std::string>
      trdispatcher_req; // DataFilter -> TRDispatcher (request)
  std::vector<std::string> trwriter_ctrl; // DataFilter -> TRReWriter   (notify)
  std::vector<std::string>
      tr_tracking_rx; // Dispatcher -> DataFilter (tracking)

  // TRs Data lanes
  std::vector<std::string> tr_data_rx; // TR inputs (upstream → DataFilter)
  std::vector<std::string> tr_data_tx; // TR outputs (DataFilter → downstream)

  // Bookkeeping (optional)
  std::vector<std::string> bk_outputs;
  std::vector<std::string> bk_inputs;
};

} // namespace dunedaq::datafilter
#endif
