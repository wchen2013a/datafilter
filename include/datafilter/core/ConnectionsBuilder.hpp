#ifndef DATAFILTER_CONNECTIONS_BUILDER_HPP_
#define DATAFILTER_CONNECTIONS_BUILDER_HPP_

#include <string>
#include <type_traits>
#include <vector>

#include "daqdataformats/TimeSlice.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "datafilter/TimeSlice_serialization.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "dfmessages/TriggerRecord_serialization.hpp" // needed by datatype_to_string conversion
#include "iomanager/IOManager.hpp"
#include "logging/Logging.hpp"
#include <nlohmann/json.hpp>

using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;
using timeslice_ptr_t = std::unique_ptr<dunedaq::daqdataformats::TimeSlice>;

namespace dunedaq::datafilter {

/**
 * @brief Factory class for building Connections from DAL configuration.
 *
 * Extended to support both TriggerRecord and TimeSlice data types.
 */
struct ConnectionsBuilder {
  template <typename MDAL> static auto &as_ref(MDAL &mdal) {
    if constexpr (std::is_pointer_v<std::decay_t<MDAL>>) {
      return *mdal;
    } else {
      return mdal;
    }
  }

  /**
   * @brief Build Connections from a DAL module configuration.
   *
   * Parses the module's inputs and outputs, categorizing them by data type:
   *   - TriggerRecord (trigger_record_ptr_t)
   *   - TimeSlice (timeslice_ptr_t)
   *   - Handshake (control messages)
   *   - BookKeeping (metadata)
   *
   * @param mdal_handle DAL module configuration (pointer or reference)
   * @return Connections structure with all UIDs categorized
   */
  template <typename MDAL> static Connections build_from_dal(MDAL mdal_handle) {
    auto &mdal = as_ref(mdal_handle);
    Connections cx;

    TLOG() << "ConnectionsBuilder: parsing DAL configuration";

    // Get data type strings for matching
    const auto dt_tr = datatype_to_string<trigger_record_ptr_t>();
    const auto dt_ts = datatype_to_string<timeslice_ptr_t>();
    const auto dt_hs = datatype_to_string<dunedaq::datafilter::Handshake>();
    const auto dt_bk = datatype_to_string<dunedaq::datafilter::BookKeeping>();

    TLOG_DEBUG(5) << "Data type strings: TR=" << dt_tr << " TS=" << dt_ts
                  << " HS=" << dt_hs << " BK=" << dt_bk;

    // ─────────────────────────────────────────────────────────────────────────
    // Parse OUTPUTS (this module → others)
    // ─────────────────────────────────────────────────────────────────────────
    for (auto con : mdal.get_outputs()) {
      const auto &dt = con->get_data_type();
      const auto &id = con->UID();
      TLOG_DEBUG(5) << "Output: dt=" << dt << " UID=" << id;

      if (dt == dt_tr) {
        // TriggerRecord output
        cx.tr_data_tx.push_back(id);
        TLOG_DEBUG(5) << "  -> TR data output";

      } else if (dt == dt_ts) {
        // TimeSlice output
        cx.ts_data_tx.push_back(id);
        TLOG_DEBUG(5) << "  -> TS data output";

      } else if (dt == dt_bk) {
        // BookKeeping output
        cx.bk_outputs.push_back(id);
        TLOG_DEBUG(5) << "  -> BookKeeping output";

      } else if (dt == dt_hs) {
        // Handshake output - categorize by UID pattern
        categorize_handshake_output(id, cx);
      }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parse INPUTS (others → this module)
    // ─────────────────────────────────────────────────────────────────────────
    for (auto con : mdal.get_inputs()) {
      const auto &dt = con->get_data_type();
      const auto &id = con->UID();
      TLOG_DEBUG(5) << "Input: dt=" << dt << " UID=" << id;

      if (dt == dt_tr) {
        // TriggerRecord input
        cx.tr_data_rx.push_back(id);
        TLOG_DEBUG(5) << "  -> TR data input";

      } else if (dt == dt_ts) {
        // TimeSlice input
        cx.ts_data_rx.push_back(id);
        TLOG_DEBUG(5) << "  -> TS data input";

      } else if (dt == dt_bk) {
        // BookKeeping input
        cx.bk_inputs.push_back(id);
        TLOG_DEBUG(5) << "  -> BookKeeping input";

      } else if (dt == dt_hs) {
        // Handshake input - categorize by UID pattern
        categorize_handshake_input(id, cx);
      }
    }

    // Log summary
    log_connections_summary(cx);

    return cx;
  }

private:
  /**
   * @brief Categorize a Handshake output connection by its UID pattern.
   */
  static void categorize_handshake_output(const std::string &id,
                                          Connections &cx) {
    // TriggerRecord tracking
    if (id.find("TR_tracking") != std::string::npos) {
      cx.tr_tracking_tx.push_back(id);
      TLOG_DEBUG(5) << "  -> TR tracking output";
    }
    // TimeSlice tracking
    else if (id.find("TS_tracking") != std::string::npos) {
      cx.ts_tracking_tx.push_back(id);
      TLOG_DEBUG(5) << "  -> TS tracking output";
    }
    // TR Writer control
    else if (id.find("trwriter") != std::string::npos) {
      cx.trwriter_ctrl.push_back(id);
      TLOG_DEBUG(5) << "  -> TR writer control output";
    }
    // TS Writer control
    else if (id.find("tswriter") != std::string::npos) {
      cx.tswriter_ctrl.push_back(id);
      TLOG_DEBUG(5) << "  -> TS writer control output";
    }
    // TR Dispatcher request
    else if (id.find("trdispatcher") != std::string::npos) {
      cx.trdispatcher_req_tx.push_back(id);
      TLOG_DEBUG(5) << "  -> TR dispatcher request output";
    }
    // TS Dispatcher request
    else if (id.find("tsdispatcher") != std::string::npos) {
      cx.tsdispatcher_req_tx.push_back(id);
      TLOG_DEBUG(5) << "  -> TS dispatcher request output";
    }
    // FilterOrchestrator control
    else if (id.find("FO_ctrl") != std::string::npos) {
      cx.fo_ctrl.push_back(id);
      TLOG_DEBUG(5) << "  -> FO control output";
    }
    // Unknown handshake output
    else {
      TLOG_DEBUG(5) << "  -> Unknown handshake output (not categorized)";
    }
  }

  /**
   * @brief Categorize a Handshake input connection by its UID pattern.
   */
  static void categorize_handshake_input(const std::string &id,
                                         Connections &cx) {
    // TriggerRecord tracking
    if (id.find("TR_tracking") != std::string::npos) {
      cx.tr_tracking_rx.push_back(id);
      TLOG_DEBUG(5) << "  -> TR tracking input";
    }
    // TimeSlice tracking
    else if (id.find("TS_tracking") != std::string::npos) {
      cx.ts_tracking_rx.push_back(id);
      TLOG_DEBUG(5) << "  -> TS tracking input";
    }
    // TR Dispatcher request
    else if (id.find("trdispatcher") != std::string::npos) {
      cx.trdispatcher_req_rx.push_back(id);
      TLOG_DEBUG(5) << "  -> TR dispatcher request input";
    }
    // TS Dispatcher request
    else if (id.find("tsdispatcher") != std::string::npos) {
      cx.tsdispatcher_req_rx.push_back(id);
      TLOG_DEBUG(5) << "  -> TS dispatcher request input";
    }
    // TR Writer control
    else if (id.find("trwriter") != std::string::npos) {
      cx.trwriter_ctrl.push_back(id);
      TLOG_DEBUG(5) << "  -> TR writer control input";
    }
    // TS Writer control
    else if (id.find("tswriter") != std::string::npos) {
      cx.tswriter_ctrl.push_back(id);
      TLOG_DEBUG(5) << "  -> TS writer control input";
    }
    // Unknown handshake input
    else {
      TLOG_DEBUG(5) << "  -> Unknown handshake input (not categorized)";
    }
  }

  /**
   * @brief Log a summary of the parsed connections.
   */
  static void log_connections_summary(const Connections &cx) {
    TLOG() << "ConnectionsBuilder: Parsed connections summary:";
    TLOG() << "  Data inputs:  TR=" << cx.tr_data_rx.size()
           << " TS=" << cx.ts_data_rx.size();
    TLOG() << "  Data outputs: TR=" << cx.tr_data_tx.size()
           << " TS=" << cx.ts_data_tx.size();
    TLOG() << "  Tracking:     TR_rx=" << cx.tr_tracking_rx.size()
           << " TS_rx=" << cx.ts_tracking_rx.size();
    TLOG() << "  Dispatchers:  TR_req=" << cx.trdispatcher_req_tx.size()
           << " TS_req=" << cx.tsdispatcher_req_tx.size();
    TLOG() << "  Writers:      TR_ctrl=" << cx.trwriter_ctrl.size()
           << " TS_ctrl=" << cx.tswriter_ctrl.size();
    TLOG() << "  Bookkeeping:  in=" << cx.bk_inputs.size()
           << " out=" << cx.bk_outputs.size();

    if (cx.is_multi_input()) {
      TLOG() << "  Mode: MULTI-INPUT (TR + TS)";
    } else if (cx.has_tr_inputs()) {
      TLOG() << "  Mode: TR-only";
    } else if (cx.has_ts_inputs()) {
      TLOG() << "  Mode: TS-only";
    } else {
      TLOG() << "  Mode: NO DATA INPUTS CONFIGURED";
    }
  }
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_CONNECTIONS_BUILDER_HPP_
