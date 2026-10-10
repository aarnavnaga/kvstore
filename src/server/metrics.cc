#include "server/metrics.h"

#include <cinttypes>
#include <cmath>
#include <cstdio>

namespace kv::server {
namespace {

std::string FormatValue(double v) {
  if (std::isinf(v)) {
    return v > 0 ? "+Inf" : "-Inf";
  }
  char buf[64];
  if (v == std::floor(v) && std::fabs(v) < 1e15) {
    std::snprintf(buf, sizeof(buf), "%.0f", v);
  } else {
    std::snprintf(buf, sizeof(buf), "%.6g", v);
  }
  return buf;
}

std::string Series(const std::string& name, const std::string& labels) {
  return labels.empty() ? name : name + "{" + labels + "}";
}

std::string JoinLabels(const std::string& labels, const std::string& extra) {
  return labels.empty() ? extra : labels + "," + extra;
}

}  // namespace

std::vector<double> LatencyBuckets() {
  return {0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
          0.025,   0.05,   0.1,     0.25,   0.5,   1,      2.5,   10};
}

Histogram::Histogram(std::vector<double> bounds)
    : bounds_(std::move(bounds)), buckets_(new std::atomic<uint64_t>[bounds_.size() + 1]) {
  for (size_t i = 0; i <= bounds_.size(); ++i) {
    buckets_[i].store(0, std::memory_order_relaxed);
  }
}

void Histogram::Observe(double v) {
  size_t i = 0;
  while (i < bounds_.size() && v > bounds_[i]) {
    ++i;
  }
  buckets_[i].fetch_add(1, std::memory_order_relaxed);
  count_.fetch_add(1, std::memory_order_relaxed);
  sum_micros_.fetch_add(static_cast<uint64_t>(std::llround(v * 1e6)), std::memory_order_relaxed);
}

void Histogram::Render(const std::string& name, const std::string& labels, std::string* out) const {
  uint64_t cumulative = 0;
  for (size_t i = 0; i <= bounds_.size(); ++i) {
    cumulative += buckets_[i].load(std::memory_order_relaxed);
    const std::string le =
        "le=\"" + (i < bounds_.size() ? FormatValue(bounds_[i]) : std::string("+Inf")) + "\"";
    *out +=
        Series(name + "_bucket", JoinLabels(labels, le)) + " " + std::to_string(cumulative) + "\n";
  }
  *out += Series(name + "_sum", labels) + " " +
          FormatValue(static_cast<double>(sum_micros_.load(std::memory_order_relaxed)) / 1e6) +
          "\n";
  *out += Series(name + "_count", labels) + " " +
          std::to_string(count_.load(std::memory_order_relaxed)) + "\n";
}

Registry::Family& Registry::GetFamily(const std::string& name, const std::string& help,
                                      const std::string& type) {
  Family& f = families_[name];
  if (f.type.empty()) {
    f.help = help;
    f.type = type;
  }
  return f;
}

Counter* Registry::AddCounter(const std::string& name, const std::string& help,
                              const std::string& labels) {
  std::lock_guard lk(mu_);
  Family& f = GetFamily(name, help, "counter");
  f.counters.emplace_back(labels, std::make_unique<Counter>());
  return f.counters.back().second.get();
}

Histogram* Registry::AddHistogram(const std::string& name, const std::string& help,
                                  const std::string& labels, std::vector<double> bounds) {
  std::lock_guard lk(mu_);
  Family& f = GetFamily(name, help, "histogram");
  f.histograms.emplace_back(labels, std::make_unique<Histogram>(std::move(bounds)));
  return f.histograms.back().second.get();
}

void Registry::AddCallback(const std::string& name, const std::string& help,
                           const std::string& type, const std::string& labels,
                           std::function<double()> fn) {
  std::lock_guard lk(mu_);
  GetFamily(name, help, type).callbacks.emplace_back(labels, std::move(fn));
}

std::string Registry::Render() const {
  std::lock_guard lk(mu_);
  std::string out;
  for (const auto& [name, f] : families_) {
    out += "# HELP " + name + " " + f.help + "\n";
    out += "# TYPE " + name + " " + f.type + "\n";
    for (const auto& [labels, c] : f.counters) {
      out += Series(name, labels) + " " + std::to_string(c->value()) + "\n";
    }
    for (const auto& [labels, h] : f.histograms) {
      h->Render(name, labels, &out);
    }
    for (const auto& [labels, fn] : f.callbacks) {
      out += Series(name, labels) + " " + FormatValue(fn()) + "\n";
    }
  }
  return out;
}

}  // namespace kv::server
