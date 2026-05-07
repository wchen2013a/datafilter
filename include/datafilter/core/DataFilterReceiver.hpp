#ifndef DATAFILTER_DATAFILTERRECEIVER_HPP_
#define DATAFILTER_DATAFILTERRECEIVER_HPP_

#include "logging/Logging.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "daqdataformats/TimeSlice.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "datafilter/TimeSlice_serialization.hpp"
#include "datafilter/bookkeeping_manager.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/core/DataFilterAlgothrims.hpp"
#include "datafilter/core/DataFilterOrganiser.hpp"
#include "datafilter/node_info.hpp"
#include "datafilter/transfer_info.hpp"
#include "iomanager/IOManager.hpp"
#include "iomanager/Receiver.hpp"

namespace dunedaq::datafilter {

using trigger_record_ptr_t = std::unique_ptr<daqdataformats::TriggerRecord>;
using timeslice_ptr_t = std::unique_ptr<daqdataformats::TimeSlice>;

struct DataFilterReceiver {
  Connections cx;
  std::shared_ptr<dunedaq::datafilter::DataFilterOrganiser> organiser;
  dunedaq::datafilter::BookkeepingReceiver *bk_receiver{nullptr};
  dunedaq::datafilter::DataFilterAlgothrims m_alg;

  // Whether to subscribe to tracking lanes (Handshake) for total_tr logs
  bool attach_tracking{true};

  // Runtime state
  std::atomic<bool> started{false};

  // We keep UIDs we registered on, so we can detach cleanly in stop()
  std::vector<std::string> registered_tr_inputs;
  std::vector<std::string> registered_ts_inputs;
  std::vector<std::string> registered_tracking_inputs;

  struct ReceivedTR {
    trigger_record_ptr_t tr;
    std::string src_uid;
    std::size_t total_tr{0};
  };

  std::mutex m_in_mu;
  std::unordered_map<std::string, TransferInfo> m_in_by_src;
  TransferInfo m_in_total;

  // If true => callback ONLY enqueues; caller must call receive_tr()
  // If false (default) => callback forwards to organiser immediately (push
  // mode)
  bool queue_only{false};

  // cooperate with a handshake pull strategy
  bool pull_mode{true};      // send request_next_tr on start/top-up
  size_t prefetch_window{4}; // how many requests to issue initially

  DataFilterReceiver(Connections c, std::shared_ptr<DataFilterOrganiser> org,
                     dunedaq::datafilter::BookkeepingReceiver &bk_ref,
                     bool attach_tracking_inputs = true)
      : cx(std::move(c)), organiser(std::move(org)), bk_receiver(&bk_ref),
        attach_tracking(attach_tracking_inputs) {}

  ~DataFilterReceiver() {
    try {
      stop();
    } catch (...) {
      // never throw from a destructor
    }
    TLOG() << "BookkeepingReceiver destroyed";
    // detach before bk_receiver stops
    // if (started.load()) {
    //   try {
    //     stop();
    //   } catch (...) { /* swallow */
    //   }
    // }
    // try {
    //   bk_receiver.stop();
    // } catch (const std::exception &e) {
    //   TLOG() << "BookkeepingReceiver.stop() failed: " << e.what();
    // }
  }

private:
  std::mutex m_q;
  std::condition_variable cv_q;
  std::deque<ReceivedTR> tr_q;
  uint32_t m_total_tr{0};
  uint32_t m_total_ts{0};

public:
  // Start: register callbacks on all TR inputs (and tracking inputs if enabled)
  void start() {
    if (started.exchange(true)) {
      TLOG() << "DataFilterReceiver.start(): already started";
      return;
    }

    // Wiring diagnostics
    TLOG() << "BK RX UID: "
           << (cx.bk_inputs.empty() ? std::string("BK input is not defined")
                                    : cx.bk_inputs.front());
    TLOG() << "BK TX UID: "
           << (cx.bk_outputs.empty() ? std::string("BK output is not defined.")
                                     : cx.bk_outputs.front());

    if (bk_receiver) {
      auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(3);
      while (
          !bk_receiver->callback_registered.load(std::memory_order_acquire)) {
        if (std::chrono::steady_clock::now() > deadline) {
          TLOG()
              << "WARN: bookkeeping callback not registered yet; proceeding.";
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }

    if (cx.tr_tracking_rx.empty())
      TLOG() << "Tracking UIDs: <none>";
    else
      for (auto &tuid : cx.tr_tracking_rx)
        TLOG_DEBUG(5) << "Tracking UID: " << tuid;

    // if (organiser)
    // organiser->df_data_ready();

    // subscribe to tracking lanes to log/control total_tr info
    if (attach_tracking && !cx.tr_tracking_rx.empty()) {
      for (const auto &tuid : cx.tr_tracking_rx) {
        try {
          TLOG() << "tuid >>>>>>>>>>>>>>>>" << tuid;
          auto track_rx =
              dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(tuid);

          // type-safe function
          std::function<void(dunedaq::datafilter::Handshake)> track_cb =
              [&](dunedaq::datafilter::Handshake msg) {
                if (msg.msg_id == "next_tr") {
                  m_total_tr = msg.total_tr;
                  TLOG() << "msg.total_tr =======> " << msg.total_tr;
                } else if (msg.msg_id == "next_ts") {
                  m_total_ts = static_cast<uint32_t>(msg.total_tr);
                  TLOG() << "DataFilterReceiver: next_ts total=" << m_total_ts;
                  // Reset the once-flag on TSRewriterSink so it sends
                  // "write_ts" ctrl exactly once for this new TS cycle.
                  if (organiser && organiser->ts_writer) {
                    organiser->ts_writer->reset_ctrl_flag();
                  }
                } else {
                  organiser->request_next_tr();
                }
                TLOG_DEBUG(5)
                    << "datafilter: TR receiver callback: " << msg.msg_id;
              };

          track_rx->add_callback(track_cb);
          registered_tracking_inputs.push_back(tuid);
          TLOG() << "DataFilterReceiver: tracking attached to " << tuid;
        } catch (const std::exception &e) {
          TLOG() << "DataFilterReceiver: failed to attach tracking to " << tuid
                 << " : " << e.what();
        }
      }
    }

    organiser->request_next_tr();

    // Subscribe to all TR inputs and forward-on-arrival
    if (cx.tr_data_rx.empty()) {
      TLOG() << "DataFilterReceiver.start(): no TR inputs configured";
    }

    for (const auto &ruid : cx.tr_data_rx) {
      try {
        auto tr_rx = dunedaq::get_iom_receiver<trigger_record_ptr_t>(ruid);

        // Forward every TR immediately to the organiser; total_tr not tracked
        // here
        auto tr_cb = [org = organiser, this,
                      src = ruid](trigger_record_ptr_t &tr) {
          using clock = std::chrono::steady_clock;
          const auto processing_start = clock::now();
          const std::size_t bytes = tr ? tr->get_total_size_bytes() : 0;

          if (queue_only) {
            // Queue mode: enqueue and notify; caller will consume via
            // receive_tr()
            {
              std::lock_guard<std::mutex> lk(m_q);
              tr_q.push_back(ReceivedTR{std::move(tr), src, m_total_tr});
            }
            cv_q.notify_one();
          } else {

            const auto rebuild_start = clock::now();
            auto tr_rebuilt = m_alg.rebuild_trigger_record(tr);

            const auto rebuild_end = clock::now();

            const double rebuild_secs =
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    rebuild_end - rebuild_start)
                    .count();

            if (rebuild_secs > 0 && bytes > 0) {
              const double rebuild_mbps =
                  (static_cast<double>(bytes) * 8.0) / rebuild_secs / 1e6;
              TLOG() << "Rebuild processing rate: " << rebuild_mbps << " Mbps";
            }

            const auto &frames = m_alg.needed_vec();
            TLOG() << "Algothrims collected " << frames.size() << " frame refs";

            // MEASURE ORGANISER PROCESSING TIME
            const auto org_start = clock::now();

            if (tr_rebuilt) {
              // Push mode (default): forward immediately to organiser
              org->accepted_trigger_record2(tr_rebuilt, m_total_tr);
            } else {
              TLOG() << "DataFilterReceiver: TR dropped by filter"
                     << " (all WIBEth fragments below ADC threshold)";
            }

            const auto org_end = clock::now();

            const double org_secs =
                std::chrono::duration_cast<std::chrono::duration<double>>(
                    org_end - org_start)
                    .count();

            if (org_secs > 0 && bytes > 0) {
              const double org_mbps =
                  (static_cast<double>(bytes) * 8.0) / org_secs / 1e6;
              TLOG() << "Organiser processing rate: " << org_mbps << " Mbps";
            }

            // one-for-one top-up: always request next TR, even if this one
            // was dropped, because we consumed one pipeline slot
            if (pull_mode && org) {
              try {
                org->request_next_tr();
              } catch (const std::exception &e) {
                TLOG() << "Top-up request_next_tr failed: " << e.what();
              }
            }
          }

          // TOTAL PROCESSING TIME
          const auto processing_end = clock::now();
          const double total_secs =
              std::chrono::duration_cast<std::chrono::duration<double>>(
                  processing_end - processing_start)
                  .count();

          if (total_secs > 0 && bytes > 0) {
            const double total_mbps =
                (static_cast<double>(bytes) * 8.0) / total_secs / 1e6;
            TLOG() << "Total processing rate (througput rate): " << total_mbps
                   << " Mbps";

            // Update your EWMA here for the real transfer rate
            std::lock_guard<std::mutex> lk(m_in_mu);
            update_ewma(total_mbps, m_in_by_src[src]);
            update_ewma(total_mbps, m_in_total);

            if (bk_receiver) {
              bk_receiver->set_transfer_rate_in(
                  m_in_total.ewma_mbps.load(std::memory_order_relaxed));
            }
          }
          // Forward immediately (total_tr=0 in no-pull mode)
          // org->accepted_trigger_record2(tr, /*total_tr*/ 0);
          // org->request_next_tr();
        };

        tr_rx->add_callback(tr_cb);
        registered_tr_inputs.push_back(ruid);
        TLOG_DEBUG(5) << "DataFilterReceiver: TR input attached to " << ruid;
      } catch (const std::exception &e) {
        TLOG() << "DataFilterReceiver: failed to attach TR input to " << ruid
               << " : " << e.what();
      }
    }

    // Subscribe to all TS inputs and forward-on-arrival
    for (const auto &ruid : cx.ts_data_rx) {
      try {
        auto ts_rx = dunedaq::get_iom_receiver<timeslice_ptr_t>(ruid);

        auto ts_cb = [org = organiser, this,
                      src = ruid](timeslice_ptr_t &ts) {
          using clock = std::chrono::steady_clock;
          const auto t0 = clock::now();
          const std::size_t bytes = ts ? ts->get_total_size_bytes() : 0;

          // Pass through to organiser (no TS algorithm yet)
          org->accepted_timeslice(ts, m_total_ts);

          // Pull-mode top-up: request next TS batch after each one is forwarded.
          if (pull_mode && org) {
            try {
              org->request_next_ts();
            } catch (const std::exception &e) {
              TLOG() << "TS top-up request_next_ts failed: " << e.what();
            }
          }

          const auto t1 = clock::now();
          const double secs =
              std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0)
                  .count();
          if (secs > 0 && bytes > 0) {
            const double mbps =
                (static_cast<double>(bytes) * 8.0) / secs / 1e6;
            std::lock_guard<std::mutex> lk(m_in_mu);
            update_ewma(mbps, m_in_by_src[src]);
            update_ewma(mbps, m_in_total);
          }
        };

        ts_rx->add_callback(ts_cb);
        registered_ts_inputs.push_back(ruid);
        TLOG_DEBUG(5) << "DataFilterReceiver: TS input attached to " << ruid;
      } catch (const std::exception &e) {
        TLOG() << "DataFilterReceiver: failed to attach TS input to " << ruid
               << " : " << e.what();
      }
    }

    if (pull_mode && prefetch_window > 0 && organiser) {
      organiser->request_next_tr(prefetch_window);
    }

    TLOG_DEBUG(5) << "DataFilterReceiver.start(): done; TR inputs="
                  << registered_tr_inputs.size()
                  << " TS inputs=" << registered_ts_inputs.size()
                  << " tracking inputs=" << registered_tracking_inputs.size();
  }

private:
  bool wait_and_pop(ReceivedTR &out) {
    std::unique_lock<std::mutex> lk(m_q);
    cv_q.wait(lk, [this] { return !tr_q.empty() || !started.load(); });
    if (!started.load() && tr_q.empty())
      return false;
    out = std::move(tr_q.front());
    tr_q.pop_front();
    return true;
  }

  bool wait_and_pop(ReceivedTR &out, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(m_q);
    if (!cv_q.wait_for(lk, timeout,
                       [this] { return !tr_q.empty() || !started.load(); }))
      return false; // timeout
    if (!started.load() && tr_q.empty())
      return false;
    out = std::move(tr_q.front());
    tr_q.pop_front();
    return true;
  }

public:
  // Stop: detach callbacks from all previously registered inputs
  void stop() {
    if (!started.exchange(false)) {
      TLOG_DEBUG(5) << "DataFilterReceiver.stop(): not started";
      return;
    }
    /*
        m_in_tick_run = false;
        if (m_in_tick.joinable())
          m_in_tick.join();
    */
    // Detach TR input callbacks
    for (const auto &ruid : registered_tr_inputs) {
      try {
        auto tr_rx = dunedaq::get_iom_receiver<trigger_record_ptr_t>(ruid);
        tr_rx->remove_callback();
        TLOG_DEBUG(6) << "DataFilterReceiver: TR input detached from " << ruid;
      } catch (const std::exception &e) {
        TLOG() << "DataFilterReceiver: detach TR input failed for " << ruid
               << " : " << e.what();
      }
    }
    registered_tr_inputs.clear();

    // Detach TS input callbacks
    for (const auto &ruid : registered_ts_inputs) {
      try {
        auto ts_rx = dunedaq::get_iom_receiver<timeslice_ptr_t>(ruid);
        ts_rx->remove_callback();
        TLOG_DEBUG(6) << "DataFilterReceiver: TS input detached from " << ruid;
      } catch (const std::exception &e) {
        TLOG() << "DataFilterReceiver: detach TS input failed for " << ruid
               << " : " << e.what();
      }
    }
    registered_ts_inputs.clear();

    // Detach tracking input callbacks
    for (const auto &tuid : registered_tracking_inputs) {
      try {
        auto track_rx =
            dunedaq::get_iom_receiver<dunedaq::datafilter::Handshake>(tuid);
        track_rx->remove_callback(); // no-op if not supported / not set
        TLOG_DEBUG(5) << "DataFilterReceiver: tracking detached from " << tuid;
      } catch (const std::exception &e) {
        TLOG() << "DataFilterReceiver: detach tracking failed for " << tuid
               << " : " << e.what();
      }
    }
    registered_tracking_inputs.clear();

    // Unblock any waiting receivers
    {
      std::lock_guard<std::mutex> lk(m_q);
    }
    cv_q.notify_all();

    TLOG_DEBUG(5) << "DataFilterReceiver.stop(): done";
  }
};

} // namespace dunedaq::datafilter
#endif
