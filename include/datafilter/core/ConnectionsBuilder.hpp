#ifndef DATAFILTER_CONNECTIONS_BUILDER_HPP_
#define DATAFILTER_CONNECTIONS_BUILDER_HPP_

#include <string>
#include <type_traits>
#include <vector>

#include "daqdataformats/TriggerRecord.hpp"
#include "daqdataformats/TriggerRecordHeaderData.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "dfmessages/TriggerRecord_serialization.hpp" // needed by datatype_to_string conversion
#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"
#include <nlohmann/json.hpp>

using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;

namespace dunedaq::datafilter {

struct ConnectionsBuilder {
  template <typename MDAL> static auto &as_ref(MDAL &mdal) {
    if constexpr (std::is_pointer_v<std::decay_t<MDAL>>) {
      return *mdal;
    } else {
      return mdal;
    }
  }

  template <typename MDAL> static Connections build_from_dal(MDAL mdal_handle) {
    auto &mdal = as_ref(mdal_handle);
    Connections cx;

    TLOG() << "Connections builder";
    const auto dt_tr = datatype_to_string<trigger_record_ptr_t>();
    const auto dt_hs = datatype_to_string<dunedaq::datafilter::Handshake>();
    // the BookKeeping is still used the manual setup.
    const auto dt_bk = datatype_to_string<dunedaq::datafilter::BookKeeping>();

    // Outputs (this module -> others)
    for (auto con : mdal.get_outputs()) {
      const auto &dt = con->get_data_type();
      const auto &id = con->UID();
      TLOG() << "Output: dt=" << dt << " UID=" << id << " dt_tr " << dt_tr
             << " dt_hs " << dt_hs;

      if (dt == dt_tr)
        cx.tr_data_tx.push_back(id);
      else if (dt == dt_bk)
        cx.bk_outputs.push_back(id);
      else if (dt == dt_hs) {
        if (id.find("trwriter") != std::string::npos)
          cx.trwriter_ctrl.push_back(id);
        if (id.find("trdispatcher") != std::string::npos)
          cx.trdispatcher_req.push_back(id);
        if (id.find("FO_ctrl") != std::string::npos)
          cx.fo_ctrl.push_back(id);
      }
    }

    // Inputs (others -> this module)
    for (auto con : mdal.get_inputs()) {
      const auto &dt = con->get_data_type();
      const auto &id = con->UID();
      TLOG() << "Input:  dt=" << dt << " UID=" << id << " dt_tr " << dt_tr;

      if (dt == dt_tr)
        cx.tr_data_rx.push_back(id);
      else if (dt == dt_bk)
        cx.bk_inputs.push_back(id);
      else if (dt == dt_hs) {
        if (id.find("TR_tracking") != std::string::npos)
          cx.tr_tracking_rx.push_back(id);
        if (id.find("trdispatcher") != std::string::npos)
          cx.trdispatcher_req.push_back(id);
        if (id.find("trwriter") != std::string::npos)
          cx.trwriter_ctrl.push_back(id);
      }
    }

    return cx;
  }
};

} // namespace dunedaq::datafilter
#endif
