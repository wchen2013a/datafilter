#ifndef DATAFILTER_STRUCTS_HPP
#define DATAFILTER_STRUCTS_HPP
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility> // for std::pair
#include <variant>
#include <vector>

#include "serialization/Serialization.hpp"

namespace dunedaq {
namespace datafilter {

enum class DispatchMode {
  kDataWriter,        // get TR and TS from DataWriter
  kStorageHDF5,       // read from HDF5 file
  kGeneratedSerial,   // send_tr() then send_ts()
  kGeneratedParallel, // send_tr() || send_ts()
};

// TR lifecycle states -- defined in DataStorage Model Notes
// States 1–5 are upstream (builder/recorder), outside datafilter scope.
enum class TRStatus : uint8_t {
  kUnknown = 0,
  // -- Upstream states (for reference) --
  // kCreated          = 1,
  // kAssignedToBuilder= 2,
  // kBuilding         = 3,
  // kBuilt            = 4,
  // kRecorded         = 5,
  // -- DataFilter states --
  kAssignedToFilter = 6, // TR dispatched to filter pipeline
  kReducing = 7,         // filter algorithm running
  kReduced = 8,          // filter complete
  kReRecorded = 9,       // written to filtered HDF5
  kFileCompleted = 10,   // all TRs in batch written
  // -- Offline states (not yet implemented) --
  // kMetadataCreated  = 11,
  // kTransferred      = 12,
  // kConfirmedOffline = 13,
  // kDeletedOnline    = 14,
  // -- Error --
  kWriteFailed = 100,
};

inline const char *to_string(TRStatus s) {
  switch (s) {
  case TRStatus::kUnknown:
    return "unknown";
  case TRStatus::kAssignedToFilter:
    return "assigned_to_filter";
  case TRStatus::kReducing:
    return "reducing";
  case TRStatus::kReduced:
    return "reduced";
  case TRStatus::kReRecorded:
    return "re_recorded";
  case TRStatus::kFileCompleted:
    return "file_completed";
  case TRStatus::kWriteFailed:
    return "write_failed";
  default:
    return "unknown";
  }
}

inline TRStatus tr_status_from_string(const std::string &s) {
  if (s == "assigned_to_filter")
    return TRStatus::kAssignedToFilter;
  if (s == "reducing")
    return TRStatus::kReducing;
  if (s == "reduced")
    return TRStatus::kReduced;
  if (s == "re_recorded")
    return TRStatus::kReRecorded;
  if (s == "file_completed")
    return TRStatus::kFileCompleted;
  if (s == "write_failed")
    return TRStatus::kWriteFailed;
  // Legacy compatibility
  if (s == "send")
    return TRStatus::kAssignedToFilter;
  if (s == "written")
    return TRStatus::kReRecorded;
  if (s == "all_complete")
    return TRStatus::kFileCompleted;
  return TRStatus::kUnknown;
}

enum struct Precision { SECONDS, MILLISECONDS, MICROSECONDS, NANOSECONDS };
struct Data {
  size_t seq_number;
  size_t trigger_number;
  size_t trigger_timestamp;
  size_t run_number;
  size_t element_id;
  size_t detector_id;
  size_t error_bits;
  size_t fragment_type;
  // daqdataformats::Fragment  fragment_type;
  std::string path_header;
  int n_frames;

  size_t publisher_id;
  size_t group_id;
  size_t conn_id;

  std::vector<int> contents;

  Data() = default;
  Data(size_t seq, size_t trigger, size_t timestamp, size_t run, size_t element,
       size_t detector, size_t error, size_t fragment, std::string path,
       int nframes, size_t publisher, size_t group, size_t conn, size_t size)
      : seq_number(seq), trigger_number(trigger), trigger_timestamp(timestamp),
        run_number(run), element_id(element), detector_id(detector),
        error_bits(error), fragment_type(fragment), path_header(path),
        n_frames(nframes), publisher_id(publisher), group_id(group),
        conn_id(conn), contents(size) {}
  virtual ~Data() = default;
  Data(Data const &) = default;
  Data &operator=(Data const &) = default;
  Data(Data &&) = default;
  Data &operator=(Data &&) = default;

  DUNE_DAQ_SERIALIZE(Data, seq_number, trigger_number, trigger_timestamp,
                     run_number, element_id, detector_id, error_bits,
                     fragment_type, path_header, n_frames, publisher_id,
                     group_id, conn_id, contents);
};

struct BookKeeping {
  std::string entry_id;
  std::string conn_id;
  std::string from_id;
  std::string datafilter_id;
  std::vector<std::pair<std::string, std::string>> node{};
  std::vector<std::pair<std::string, std::string>> tr_header_info{};
  std::vector<std::pair<std::string, std::string>> file_attributes_info{};
  std::string tr_status{};

  std::vector<std::string> file_send_list{};
  // std::string file_send_fail_list{};
  std::string file_send_status{}; // sended or receive, or transit.
  double transfer_rate;
  std::string write_status{};
  unsigned int run_number;
  BookKeeping() = default;
  BookKeeping(std::string entry) : entry_id(entry) {}
  DUNE_DAQ_SERIALIZE(BookKeeping, entry_id, conn_id, from_id, datafilter_id,
                     node, tr_header_info, file_attributes_info, tr_status,
                     file_send_list, file_send_status, transfer_rate,
                     write_status, run_number);
};

// struct BookKeeping_json {
//     std::string msg_id;
//     nlohmann::json bk_info;
//     BookKeeping_json() = default;
//     BookKeeping_json(std::string msg) : msg_id(msg){};
//     DUNE_DAQ_SERIALIZE(BookKeeping_json, bk_info);
// };

struct Handshake {
  std::string msg_id;
  int total_tr{0};
  uint64_t ack_id{0};
  Handshake() = default;
  Handshake(std::string msg) : msg_id(msg) {}

  DUNE_DAQ_SERIALIZE(Handshake, msg_id, total_tr, ack_id);
};

struct time_point_to_string {
  Precision precision = Precision::SECONDS; // Default to seconds

  // Constructor (for direct initialization)
  explicit time_point_to_string(Precision prec = Precision::SECONDS)
      : precision(prec) {}

  // Conversion function
  std::string
  operator()(const std::chrono::system_clock::time_point &tp) const {
    // Convert to time_t for seconds since epoch
    std::time_t time = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&time);

    // Format the base time string (YYYY-MM-DD HH:MM:SS)
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");

    if (precision != Precision::SECONDS) {
      auto since_epoch = tp.time_since_epoch();
      auto seconds =
          std::chrono::duration_cast<std::chrono::seconds>(since_epoch);
      auto subseconds = since_epoch - seconds;

      switch (precision) {
      case Precision::MILLISECONDS: {
        auto ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(subseconds);
        oss << "." << std::setfill('0') << std::setw(3) << ms.count();
        break;
      }
      case Precision::MICROSECONDS: {
        auto us =
            std::chrono::duration_cast<std::chrono::microseconds>(subseconds);
        oss << "." << std::setfill('0') << std::setw(6) << us.count();
        break;
      }
      case Precision::NANOSECONDS: {
        auto ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(subseconds);
        oss << "." << std::setfill('0') << std::setw(9) << ns.count();
        break;
      }
      default:
        break; // SECONDS (no subsecond)
      }
    }

    return oss.str();
  }
};

} // namespace datafilter
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Data, "data_t");
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::Handshake, "init_t");
DUNE_DAQ_SERIALIZABLE(dunedaq::datafilter::BookKeeping, "bk_t");
} // namespace dunedaq
#endif
