#ifndef DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_
#define DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_

// ============================================================================
// DataFilterAlgothrims : frame-level ADC threshold filtering for WIBEth TPC.
//
// Responsibilities:
//  - Iterate all fragments in a TriggerRecord
//  - For WIBEth fragments: scan every channel/sample ADC value
//    → keep the fragment if max ADC >= adc_threshold
//    → drop the fragment otherwise (log the rejection)
//  - Non-WIBEth fragments (DAPHNE, TA, TC, …) are kept unconditionally
//  - Rebuild and return a new TriggerRecord from the surviving fragments
//  - Return nullptr if no fragments survive (caller drops the TR entirely)
// ============================================================================

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "daqdataformats/Fragment.hpp"
#include "daqdataformats/FragmentHeader.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "daqdataformats/TriggerRecordHeader.hpp"
#include "fddetdataformats/WIBEthFrame.hpp"
#include "logging/Logging.hpp"

namespace dunedaq::datafilter {

using trigger_record_ptr_t = std::unique_ptr<daqdataformats::TriggerRecord>;

// ─────────────────────────────────────────────────────────────────────────────
// FrameRef — lightweight, non-owning view into one fragment's payload bytes
// ─────────────────────────────────────────────────────────────────────────────
struct FrameRef {
  const std::uint8_t *data{
      nullptr};        // pointer to payload (after fragment header)
  std::size_t size{0}; // payload size in bytes

  daqdataformats::SourceID source_id{};
  std::uint64_t trigger_number{0};
  std::uint64_t trigger_timestamp{0};
};

// ─────────────────────────────────────────────────────────────────────────────
// DataFilterAlgothrims
// ─────────────────────────────────────────────────────────────────────────────
struct DataFilterAlgothrims {

  // ADC rejection threshold — configured via OKS (DataFilter.adc_threshold).
  // A WIBEth fragment is KEPT if any channel/sample has ADC >= this value.
  // Set to 0 to keep all fragments (pass-through behaviour).
  uint16_t adc_threshold{0};

  const std::vector<FrameRef> &needed_vec() const noexcept {
    return m_needed_vec;
  }
  void clear_vec() { m_needed_vec.clear(); }

  // ──────────────────────────────────────────────────────────────
  // Entry point called by DataFilterReceiver for every incoming TR
  // ──────────────────────────────────────────────────────────────
  inline trigger_record_ptr_t
  rebuild_trigger_record(trigger_record_ptr_t &tr) const {
    m_needed_vec.clear();

    if (!tr) {
      TLOG_DEBUG(5)
          << "DataFilterAlgothrims::rebuild_trigger_record(): null TR";
      return nullptr;
    }

    const auto &frags = tr->get_fragments_ref();
    if (frags.empty()) {
      TLOG() << "DataFilterAlgothrims: TR has no fragments, dropping";
      return nullptr;
    }
    TLOG() << "DataFilterAlgothrims: processing TR with " << frags.size()
           << " fragments, adc_threshold=" << adc_threshold;

    // Collect trigger metadata from first fragment
    std::uint64_t trig_num{0};
    std::uint64_t trig_ts{0};
    if (frags.at(0)) {
      trig_num = frags.at(0)->get_trigger_number();
      trig_ts = frags.at(0)->get_trigger_timestamp();
    }

    // Populate m_needed_vec (non-owning views)
    extract_frames_from_tr(*tr, trig_num, trig_ts);

    // ── Fragment-level filtering ──────────────────────────────────
    // Policy: if the TR contains WIBEth fragments and ALL of them fail the ADC
    // threshold, drop the entire TR (including non-WIBEth payload fragments).
    // If there are no WIBEth fragments at all, keep the TR as-is.
    std::vector<std::size_t> wibeth_keep;   // WIBEth indices that passed
    std::vector<std::size_t> nonwibeth_idx; // non-WIBEth indices (kept if any WIBEth passes)
    std::size_t n_wibeth = 0;

    for (std::size_t i = 0; i < frags.size(); ++i) {
      const auto &fptr = frags[i];
      if (!fptr) {
        TLOG_DEBUG(5) << "DataFilterAlgothrims: null fragment at index " << i
                      << ", skipping";
        continue;
      }

      const auto ftype = fptr->get_fragment_type();
      if (ftype != daqdataformats::FragmentType::kWIBEth) {
        // Non-WIBEth fragment: collect; include only if some WIBEth passes
        TLOG_DEBUG(5) << "DataFilterAlgothrims: non-WIBEth fragment"
               << " source_id=" << fptr->get_element_id()
               << " fragment_type=" << static_cast<int>(ftype)
               << " trigger=" << trig_num;
        nonwibeth_idx.push_back(i);
        continue;
      }

      ++n_wibeth;
      // WIBEth fragment: scan ADC values
      if (passes_adc_threshold(*fptr)) {
        wibeth_keep.push_back(i);
      } else {
        TLOG() << "DataFilterAlgothrims: dropping WIBEth fragment"
               << " source_id=" << fptr->get_element_id()
               << " trigger=" << trig_num << " (max ADC < " << adc_threshold
               << ")";
      }
    }

    // Decision: if there were WIBEth fragments but none passed → drop whole TR
    if (n_wibeth > 0 && wibeth_keep.empty()) {
      TLOG() << "DataFilterAlgothrims: all " << n_wibeth
             << " WIBEth fragments rejected for trigger=" << trig_num
             << ", dropping TR";
      return nullptr;
    }

    // Build final keep list: passing WIBEth + non-WIBEth (only when WIBEth passed)
    std::vector<std::size_t> keep_indices;
    keep_indices.reserve(wibeth_keep.size() + nonwibeth_idx.size());
    keep_indices.insert(keep_indices.end(), wibeth_keep.begin(), wibeth_keep.end());
    keep_indices.insert(keep_indices.end(), nonwibeth_idx.begin(), nonwibeth_idx.end());

    if (keep_indices.empty()) {
      TLOG() << "DataFilterAlgothrims: no fragments to keep for trigger="
             << trig_num << " (no WIBEth, no other fragments), dropping TR";
      return nullptr;
    }

    // ── Rebuild TriggerRecord from kept fragments ─────────────────
    auto new_tr =
        std::make_unique<daqdataformats::TriggerRecord>(tr->get_header_ref());

    // Move fragments from the original TR into the new one.
    // get_fragments_ref() returns a non-const ref on a non-const TR.
    auto &mutable_frags = tr->get_fragments_ref();
    for (std::size_t idx : keep_indices) {
      new_tr->add_fragment(std::move(mutable_frags[idx]));
    }

    TLOG() << "DataFilterAlgothrims: trigger=" << trig_num << " kept "
           << keep_indices.size() << "/" << frags.size() << " fragments";

    return new_tr;
  }

private:
  mutable std::vector<FrameRef> m_needed_vec;

  // ── Populate m_needed_vec with non-owning payload views ──────────
  inline void extract_frames_from_tr(const daqdataformats::TriggerRecord &tr,
                                     std::uint64_t trig_num,
                                     std::uint64_t trig_ts) const {
    const auto &frags = tr.get_fragments_ref();
    m_needed_vec.reserve(m_needed_vec.size() + frags.size());

    for (const auto &fptr : frags) {
      if (!fptr)
        continue;
      const daqdataformats::Fragment &frag = *fptr;

      FrameRef ref;
      ref.data = static_cast<const std::uint8_t *>(frag.get_data());
      ref.size = frag.get_data_size();
      ref.source_id = frag.get_element_id();
      ref.trigger_number = trig_num;
      ref.trigger_timestamp = trig_ts;

      m_needed_vec.emplace_back(ref);
    }
  }

  // ── ADC threshold check for one WIBEth fragment ──────────────────
  // Returns true if the fragment has at least one ADC sample >=
  // adc_threshold.
  inline bool passes_adc_threshold(const daqdataformats::Fragment &frag) const {
    using WIBEthFrame = fddetdataformats::WIBEthFrame;

    const auto *payload = static_cast<const uint8_t *>(frag.get_data());
    const std::size_t n_bytes = frag.get_data_size();

    // Guard: payload must hold at least one complete frame
    if (n_bytes < sizeof(WIBEthFrame)) {
      TLOG() << "DataFilterAlgothrims: WIBEth fragment payload too small ("
             << n_bytes << " B < " << sizeof(WIBEthFrame)
             << " B), keeping unconditionally";
      return true;
    }

    const std::size_t n_frames = n_bytes / sizeof(WIBEthFrame);
    uint16_t max_adc = 0;

    for (std::size_t fi = 0; fi < n_frames; ++fi) {
      const auto *frame = reinterpret_cast<const WIBEthFrame *>(
          payload + fi * sizeof(WIBEthFrame));

      for (int ch = 0; ch < WIBEthFrame::s_num_channels; ++ch) {
        for (int sample = 0; sample < WIBEthFrame::s_time_samples_per_frame;
             ++sample) {
          const uint16_t adc = frame->get_adc(ch, sample);
          if (adc > max_adc)
            max_adc = adc;
          if (max_adc >= adc_threshold)
            return true; // early exit
        }
      }
    }

    TLOG_DEBUG(5) << "DataFilterAlgothrims: fragment max_adc=" << max_adc
                  << " < threshold=" << adc_threshold;
    return false;
  }
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_
