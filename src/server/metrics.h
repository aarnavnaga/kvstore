#pragma once

// A small Prometheus registry: counters, histograms and callback gauges, rendered in the
// text exposition format (version 0.0.4). Recording a sample is a relaxed atomic add, so
// the hot path never takes a lock; only rendering walks the registry under its mutex.

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace kv::server {

class Counter {
 public:
  void Inc(uint64_t n = 1) { value_.fetch_add(n, std::memory_order_relaxed); }
  [[nodiscard]] uint64_t value() const { return value_.load(std::memory_order_relaxed); }

 private:
  std::atomic<uint64_t> value_{0};
};

class Histogram {
 public:
  explicit Histogram(std::vector<double> bounds);
  void Observe(double v);
  void Render(const std::string& name, const std::string& labels, std::string* out) const;

 private:
  std::vector<double> bounds_;  // bucket upper bounds, ascending; +Inf is implicit
  std::unique_ptr<std::atomic<uint64_t>[]> buckets_;
  std::atomic<uint64_t> count_{0};
  std::atomic<uint64_t> sum_micros_{0};  // sum kept as integer micro-units: atomic add, no CAS
};

class Registry {
 public:
  // Each call registers one series. labels is the inside of {...}, e.g. op="put".
  Counter* AddCounter(const std::string& name, const std::string& help,
                      const std::string& labels = "");
  Histogram* AddHistogram(const std::string& name, const std::string& help,
                          const std::string& labels, std::vector<double> bounds);
  // A gauge (or a counter owned elsewhere) whose value is read when scraped.
  void AddCallback(const std::string& name, const std::string& help, const std::string& type,
                   const std::string& labels, std::function<double()> fn);

  [[nodiscard]] std::string Render() const;

 private:
  struct Family {
    std::string help;
    std::string type;
    std::vector<std::pair<std::string, std::unique_ptr<Counter>>> counters;
    std::vector<std::pair<std::string, std::unique_ptr<Histogram>>> histograms;
    std::vector<std::pair<std::string, std::function<double()>>> callbacks;
  };
  Family& GetFamily(const std::string& name, const std::string& help, const std::string& type);

  mutable std::mutex mu_;
  std::map<std::string, Family> families_;
};

// LatencyBuckets covers 50 us to 10 s, roughly 2.5x apart.
std::vector<double> LatencyBuckets();

}  // namespace kv::server
