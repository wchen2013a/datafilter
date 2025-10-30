#ifndef DATAFILTER_DATAFILTERORGANISER_HPP_
#define DATAFILTER_DATAFILTERORGANISER_HPP_

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "daqdataformats/TriggerRecord.hpp"
// #include "datafilter/config/DataFilterConfigAdapter.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/core/DataFilterTRSink.hpp"
#include "iomanager/IOManager.hpp"
#include "iomanager/Sender.hpp"
#include "logging/Logging.hpp"

#include <memory>

namespace dunedaq::datafilter {
using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;

struct DataFilterOrganiser {
  Connections cx;
  std::shared_ptr<dunedaq::datafilter::TRRewriterSink> writer;

  DataFilterOrganiser(Connections conns, std::shared_ptr<TRRewriterSink> w)
      : cx(std::move(conns)), writer(std::move(w)) {}

  inline void df_data_ready() {
    if (cx.fo_ctrl.empty()) {
      TLOG() << "Organiser::df_ready(): no FO_ctrl endpoints";
      return;
    }
    for (const auto &uid : cx.fo_ctrl) {
      try {
        auto orch_ctrl =
            dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(uid);
        dunedaq::datafilter::Handshake h("df_data_ready");
        // h.total_tr = prefetch_window > 0 ? prefetch_window : 4;
        orch_ctrl->send(std::move(h), dunedaq::iomanager::Sender::s_no_block);
        TLOG() << "DF: notified orchestrator DF is data - ready; window = "
               << h.total_tr;
      } catch (const std::exception &e) {
        TLOG() << "DF: failed to notify orchestrator: " << e.what();
      }
    }
  }

  // Send ONE "next_tr" request to every configured TRDispatcher control UID.
  inline void request_next_tr() {
    if (cx.trdispatcher_req.empty()) {
      TLOG() << "Organiser::request_next_tr(): no dispatcher request endpoints "
                "configured";
      return;
    }

    for (const auto &uid : cx.trdispatcher_req) {
      try {
        auto s = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(uid);
        dunedaq::datafilter::Handshake msg("next_tr");
        s->send(std::move(msg), dunedaq::iomanager::Sender::s_block);
        TLOG() << "Organiser::request_next_tr() sent to FilterOrchastrator "
                  "with uid "
               << uid;
      } catch (const std::exception &e) {
        TLOG() << "Organiser::request_next_tr() failed on " << uid << " : "
               << e.what();
      }
    }
  }

  // Send 'count' "next_tr" requests (useful for simple prefetch).
  // Each iteration sends to ALL configured dispatcher UIDs.
  inline void request_next_tr(std::size_t count) {
    if (count == 0)
      return;
    if (cx.trdispatcher_req.empty()) {
      TLOG() << "Organiser::request_next_tr(count): no dispatcher request "
                "endpoints configured";
      return;
    }

    for (std::size_t i = 0; i < count; ++i) {
      for (const auto &uid : cx.trdispatcher_req) {
        try {
          auto s = dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(uid);
          dunedaq::datafilter::Handshake msg("next_tr");
          s->send(std::move(msg), dunedaq::iomanager::Sender::s_block);
          TLOG_DEBUG(6) << "Organiser::request_next_tr(" << count << "): sent #"
                        << (i + 1) << " to " << uid;
        } catch (const std::exception &e) {
          TLOG() << "Organiser::request_next_tr(" << count << ") failed on "
                 << uid << " : " << e.what();
        }
      }
    }
  }

  // Forward a TR to the configured sink with total number of TR .
  inline void accepted_trigger_record2(trigger_record_ptr_t &tr,
                                       std::size_t total_tr) {
    if (!writer) {
      TLOG() << "Organiser::accepted_trigger_record2(): writer not set";
      return;
    }
    writer->send_tr(tr, total_tr);
  }
};

} // namespace dunedaq::datafilter
#endif
