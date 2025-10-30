#ifndef DATAFILTER_TRANSFERRATE_HPP_
#define DATAFILTER_TRANSFERRATE_HPP_

#include <atomic>
#include <chrono>

namespace dunedaq::datafilter {

struct TransferInfo {
  std::atomic<double> last_mbps{0.0}; // instantaneous
  std::atomic<double> ewma_mbps{0.0}; // smoothed using EWMA
};

// using Exponentially Weighted Moving Average. alpha is an user parameter
// [0..1]
inline void update_ewma(double sample, TransferInfo &s, double alpha = 0.3) {
  s.last_mbps.store(sample, std::memory_order_relaxed);
  const double prev = s.ewma_mbps.load(std::memory_order_relaxed);
  const double next =
      (prev == 0.0) ? sample : (alpha * sample + (1.0 - alpha) * prev);
  s.ewma_mbps.store(next, std::memory_order_relaxed);
}

} // namespace dunedaq::datafilter
#endif
