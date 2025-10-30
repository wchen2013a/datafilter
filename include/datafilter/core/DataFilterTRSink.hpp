#ifndef DATAFILTER_DATAFILTERTRSINK_HPP_
#define DATAFILTER_DATAFILTERTRSINK_HPP_

#include "daqdataformats/TriggerRecord.hpp"
#include <memory>

#include "daqdataformats/TriggerRecord.hpp"
#include "iomanager/IOManager.hpp"
#include "iomanager/Sender.hpp"

#include "datafilter/bookkeeping_manager.hpp"
#include "datafilter/core/Connections.hpp"
#include "datafilter/datafilter_structs.hpp"
#include "datafilter/transfer_info.hpp"

namespace dunedaq::datafilter {
using trigger_record_ptr_t =
    std::unique_ptr<dunedaq::daqdataformats::TriggerRecord>;

struct DataFilterTRSink {
  virtual ~DataFilterTRSink() = default;
  virtual void send_tr(trigger_record_ptr_t &tr, size_t total_tr) = 0;
};

// Selection policy for multiple tr_data_tx outputs.
enum class SendPolicy {
  First,        // send to the first configured tr_data_tx
  RoundRobin,   // RB rotates across tr_data_tx entries
  HashByTrigger // choose by (trigger_number % N)
};

struct TRRewriterSink : DataFilterTRSink {
  Connections cx;
  SendPolicy policy{SendPolicy::First};

  // state for RoundRobin
  std::atomic<std::size_t> rr_index{0};
  TransferInfo m_out;

  std::shared_ptr<dunedaq::datafilter::BookkeepingReceiver> m_bk;

  explicit TRRewriterSink(Connections conns, SendPolicy p = SendPolicy::First)
      : cx(std::move(conns)), policy(p) {}

  inline void init(const std::string &data_uid, const std::string &ctrl_uid) {
    m_data_uid = data_uid;
    m_ctrl_uid = ctrl_uid;
    m_tr_sender =
        dunedaq::get_iom_sender<std::unique_ptr<daqdataformats::TriggerRecord>>(
            m_data_uid);
    m_ctrl_sender =
        dunedaq::get_iom_sender<dunedaq::datafilter::Handshake>(m_ctrl_uid);
    TLOG()
        << "TRRewriterSink: bound data uid=" << m_data_uid << " type="
        << datatype_to_string<std::unique_ptr<daqdataformats::TriggerRecord>>();
  }

  void bind_bookkeeping(
      std::shared_ptr<dunedaq::datafilter::BookkeepingReceiver> bk) {
    m_bk = bk;
  }
  inline void send_tr(trigger_record_ptr_t &tr, std::size_t total_tr) override {
    using clock = std::chrono::steady_clock;
    const auto t0 = clock::now();
    // TR rewriter control: notify downstream we’re about to send
    const auto bytes = tr ? tr->get_total_size_bytes() : 0;
    TLOG() << "TR total size in bytes " << bytes;

    const std::string &tx_uid = cx.tr_data_tx.front();
    TLOG() << "tx_uid " << tx_uid;

    trigger_record_ptr_t tr_out = std::move(tr);
    if (!tr_out) {
      TLOG() << "send_tr(): NULL TriggerRecord, nothing to send";
      return;
    }

    TLOG() << "TR rewriter send TR to FilterResultWriter";
    if (!cx.trwriter_ctrl.empty()) {
      const auto &ctrl_uid = cx.trwriter_ctrl.front();
      TLOG() << "ctrl_uid " << ctrl_uid;
      try {
        dunedaq::datafilter::Handshake h("write_tr");
        h.total_tr = total_tr;
        m_ctrl_sender->send(std::move(h),
                            dunedaq::iomanager::Sender::s_no_block);
        TLOG() << "TRRewriterSink: wrote ctrl 'write_tr' to " << ctrl_uid
               << " total_tr=" << total_tr;
      } catch (const std::exception &e) {
        TLOG() << "TRRewriterSink: ctrl send failed on " << ctrl_uid << " : "
               << e.what();
      }
    }

    // Send the TR
    if (cx.tr_data_tx.empty()) {
      TLOG() << "TRRewriterSink: No tr_data_tx outputs configured; dropping TR";
      return;
    }

    // this is not enable yet.
    // const std::string &tx_uid = pick_tx_uid(tr);
    try {
      m_tr_sender->send(std::move(tr_out), dunedaq::iomanager::Sender::s_block);
      TLOG_DEBUG(5) << "TRRewriterSink: TR sent on " << tx_uid;
    } catch (const std::exception &e) {
      TLOG() << "TRRewriterSink: ERROR sending TR on " << tx_uid << " : "
             << e.what();
    }

    const auto t1 = clock::now();
    const double s =
        std::chrono::duration_cast<std::chrono::duration<double>>(t1 - t0)
            .count();
    if (s > 0.0 && bytes > 0) {
      const double mbps = (static_cast<double>(bytes) * 8.0) / s / 1e6;
      update_ewma(mbps, m_out);
      if (m_bk)
        m_bk->set_transfer_rate_out(
            m_out.ewma_mbps.load(std::memory_order_relaxed));
    }
  }

private:
  std::string m_data_uid, m_ctrl_uid;
  std::shared_ptr<dunedaq::iomanager::SenderConcept<
      std::unique_ptr<daqdataformats::TriggerRecord>>>
      m_tr_sender;

  std::shared_ptr<
      dunedaq::iomanager::SenderConcept<dunedaq::datafilter::Handshake>>
      m_ctrl_sender;

  inline const std::string &pick_tx_uid(const trigger_record_ptr_t &tr) {
    switch (policy) {
    case SendPolicy::First:
      return cx.tr_data_tx.front();

    case SendPolicy::RoundRobin: {
      auto i = rr_index.fetch_add(1);
      return cx.tr_data_tx[i % cx.tr_data_tx.size()];
    }

    case SendPolicy::HashByTrigger: {
      std::size_t idx = 0;
      if (tr && !tr->get_fragments_ref().empty()) {
        const auto &f0 = tr->get_fragments_ref().at(0);
        idx = static_cast<std::size_t>(f0->get_trigger_number() %
                                       cx.tr_data_tx.size());
      }
      return cx.tr_data_tx[idx];
    }
    }

    // Fallback to First (this is the default policy)
    return cx.tr_data_tx.front();
  }
};

} // namespace dunedaq::datafilter
#endif
