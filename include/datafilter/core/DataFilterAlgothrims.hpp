#ifndef DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_
#define DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_

// ============================================================================
// DataFilterAlgothrims : this is not implemented yet. More works are needed.
//
// Responsibilities (initial):
//  - Take ownership of a TriggerRecord (TR)
//  - Extract each fragment's payload ("frame") into a vector for later analysis
//  - Provide rebuild_trigger_record(...) entry-point to (later)
//  transform/rebuild
//    the TR (currently a no-op pass-through)
// ============================================================================

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "daqdataformats/Fragment.hpp"
#include "daqdataformats/TriggerRecord.hpp"
#include "logging/Logging.hpp"

namespace dunedaq::datafilter {

using trigger_record_ptr_t = std::unique_ptr<daqdataformats::TriggerRecord>;

struct FrameRef {
  const std::uint8_t *data{nullptr}; // beginning of payload bytes
  std::size_t size{0};               // payload size in bytes

  daqdataformats::SourceID source_id{};
  std::uint64_t trigger_number{0};
  std::uint64_t trigger_timestamp{0};
};

struct DataFilterAlgothrims {
  // Public: the vector that later algorithms will analyze.
  const std::vector<FrameRef> &needed_vec() const noexcept {
    return m_needed_vec;
  }
  void clear_vec() { m_needed_vec.clear(); }

  inline trigger_record_ptr_t
  rebuild_trigger_record(trigger_record_ptr_t &tr) const {
    m_needed_vec.clear();

    if (!tr) {
      TLOG_DEBUG(5)
          << "DataFilterAlgothrims::rebuild_trigger_record(): null TR";
      return std::move(tr);
    }

    // collect basic tagging (trigger number/timestamp)
    std::uint64_t trig_num{0};
    std::uint64_t trig_ts{0};
    if (!tr->get_fragments_ref().empty()) {
      const auto &f0 = tr->get_fragments_ref().at(0);
      if (f0) {
        trig_num = f0->get_trigger_number();
        trig_ts = f0->get_trigger_timestamp();
      }
    }

    // Extract a FrameRef per fragment
    // extract_frames_from_tr(*tr, trig_num, trig_ts);

    // Algorithms operation here
    //  Inspect/modify frames and rebuild fragments
    //  Construct a new TriggerRecord
    //  Return the rebuilt TR

    return std::move(tr); // pass-through for now
  }

private:
  // Internal vector
  mutable std::vector<FrameRef> m_needed_vec;

  // Extract frames (non-owning) from all fragments in a TR
  inline void extract_frames_from_tr(const daqdataformats::TriggerRecord &tr,
                                     std::uint64_t trig_num,
                                     std::uint64_t trig_ts) {
    const auto &frags = tr.get_fragments_ref();
    m_needed_vec.reserve(m_needed_vec.size() + frags.size());

    for (const auto &fptr : frags) {
      if (!fptr) {
        TLOG_DEBUG(5) << "DataFilterAlgothrims: encountered null Fragment*";
        continue;
      }
      const daqdataformats::Fragment &frag = *fptr;

      const auto *payload = static_cast<const std::uint8_t *>(frag.get_data());
      std::size_t payload_size = frag.get_data_size();

      FrameRef ref;
      ref.data = payload;
      ref.size = payload_size;
      ref.source_id = frag.get_element_id(); // SourceID tag
      ref.trigger_number = trig_num;         // propagated from TR
      ref.trigger_timestamp = trig_ts;

      m_needed_vec.emplace_back(ref);
    }
  }
};

} // namespace dunedaq::datafilter

#endif // DATAFILTER_CORE_DATAFILTERALGOTHRIMS_HPP_
