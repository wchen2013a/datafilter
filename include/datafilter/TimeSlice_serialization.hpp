/**
 * @file datafilter/TimeSlice_serialization.hpp TimeSlice MsgPack serialization
 *
 * dfmessages does not provide TimeSlice serialization (only TriggerRecord).
 * This header fills that gap so IOManager can send/receive TimeSlice objects.
 *
 * Modeled after dfmessages/TriggerRecord_serialization.hpp.
 */

#ifndef DATAFILTER_INCLUDE_DATAFILTER_TIMESLICE_SERIALIZATION_HPP_
#define DATAFILTER_INCLUDE_DATAFILTER_TIMESLICE_SERIALIZATION_HPP_

#include "daqdataformats/TimeSlice.hpp"
#include "dfmessages/Fragment_serialization.hpp"
#include "dfmessages/SourceID_serialization.hpp"
#include "serialization/Serialization.hpp"

#include <memory>
#include <utility>
#include <vector>

// ─────────────────────────────────────────────────────────────────────────────
// TimeSliceHeader  (plain struct → serialize individual fields)
// ─────────────────────────────────────────────────────────────────────────────
namespace msgpack {
MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
{
  namespace adaptor {

  template<>
  struct pack<dunedaq::daqdataformats::TimeSliceHeader>
  {
    template<typename Stream>
    packer<Stream>& operator()(msgpack::packer<Stream>& o,
                               dunedaq::daqdataformats::TimeSliceHeader const& h) const
    {
      o.pack_array(5);
      o.pack(h.timeslice_header_marker);
      o.pack(h.version);
      o.pack(h.timeslice_number);
      o.pack(h.run_number);
      o.pack(h.element_id);
      return o;
    }
  };

  template<>
  struct as<dunedaq::daqdataformats::TimeSliceHeader>
  {
    dunedaq::daqdataformats::TimeSliceHeader operator()(msgpack::object const& o) const
    {
      dunedaq::daqdataformats::TimeSliceHeader h;
      h.timeslice_header_marker = o.via.array.ptr[0].as<uint32_t>();
      h.version                 = o.via.array.ptr[1].as<uint32_t>();
      h.timeslice_number        = o.via.array.ptr[2].as<dunedaq::daqdataformats::timeslice_number_t>();
      h.run_number              = o.via.array.ptr[3].as<dunedaq::daqdataformats::run_number_t>();
      h.element_id              = o.via.array.ptr[4].as<dunedaq::daqdataformats::SourceID>();
      return h;
    }
  };

  } // namespace adaptor
} // namespace MSGPACK_DEFAULT_API_NS
} // namespace msgpack

DUNE_DAQ_SERIALIZABLE(dunedaq::daqdataformats::TimeSliceHeader, "TimeSliceHeader");

// ─────────────────────────────────────────────────────────────────────────────
// TimeSlice  (header + vector<unique_ptr<Fragment>>, same layout as TriggerRecord)
// ─────────────────────────────────────────────────────────────────────────────
namespace msgpack {
MSGPACK_API_VERSION_NAMESPACE(MSGPACK_DEFAULT_API_NS)
{
  namespace adaptor {

  template<>
  struct pack<dunedaq::daqdataformats::TimeSlice>
  {
    template<typename Stream>
    packer<Stream>& operator()(msgpack::packer<Stream>& o,
                               dunedaq::daqdataformats::TimeSlice const& ts) const
    {
      auto header = ts.get_header();
      auto& frags = ts.get_fragments_ref();

      o.pack_array(1 + frags.size());
      o.pack(header);
      for (auto& fragptr : frags) {
        o.pack(fragptr);
      }
      return o;
    }
  };

  template<>
  struct as<dunedaq::daqdataformats::TimeSlice>
  {
    dunedaq::daqdataformats::TimeSlice operator()(msgpack::object const& o) const
    {
      dunedaq::daqdataformats::TimeSlice ts(
        o.via.array.ptr[0].as<dunedaq::daqdataformats::TimeSliceHeader>());

      for (size_t ii = 1; ii < o.via.array.size; ++ii) {
        auto fragptr = o.via.array.ptr[ii].as<std::unique_ptr<dunedaq::daqdataformats::Fragment>>();
        ts.add_fragment(std::move(fragptr));
      }
      return ts;
    }
  };

  template<>
  struct pack<std::unique_ptr<dunedaq::daqdataformats::TimeSlice>>
  {
    template<typename Stream>
    packer<Stream>& operator()(msgpack::packer<Stream>& o,
                               std::unique_ptr<dunedaq::daqdataformats::TimeSlice> const& ts) const
    {
      auto header = ts->get_header();
      auto& frags = ts->get_fragments_ref();

      o.pack_array(1 + frags.size());
      o.pack(header);
      for (auto& fragptr : frags) {
        o.pack(fragptr);
      }
      return o;
    }
  };

  template<>
  struct as<std::unique_ptr<dunedaq::daqdataformats::TimeSlice>>
  {
    std::unique_ptr<dunedaq::daqdataformats::TimeSlice> operator()(msgpack::object const& o) const
    {
      auto ts = std::make_unique<dunedaq::daqdataformats::TimeSlice>(
        o.via.array.ptr[0].as<dunedaq::daqdataformats::TimeSliceHeader>());

      for (size_t ii = 1; ii < o.via.array.size; ++ii) {
        auto fragptr = o.via.array.ptr[ii].as<std::unique_ptr<dunedaq::daqdataformats::Fragment>>();
        ts->add_fragment(std::move(fragptr));
      }
      return ts;
    }
  };

  } // namespace adaptor
} // namespace MSGPACK_DEFAULT_API_NS
} // namespace msgpack

DUNE_DAQ_SERIALIZABLE(dunedaq::daqdataformats::TimeSlice, "TimeSlice");
DUNE_DAQ_SERIALIZABLE(std::unique_ptr<dunedaq::daqdataformats::TimeSlice>, "TimeSlice");

#endif // DATAFILTER_INCLUDE_DATAFILTER_TIMESLICE_SERIALIZATION_HPP_
