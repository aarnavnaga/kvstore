#pragma once

// YCSB's scrambled Zipfian key chooser: rank r is drawn with probability proportional to
// 1 / r^theta, then hashed so the popular keys are spread across the keyspace instead of
// clustered at the low end (which would make one SSTable block absurdly hot).

#include <cmath>
#include <cstdint>
#include <random>

namespace kv::bench {

class ScrambledZipfian {
 public:
  explicit ScrambledZipfian(uint64_t items, double theta = 0.99) : items_(items), theta_(theta) {
    // zeta(n) is O(n) to compute; done once per generator.
    for (uint64_t i = 1; i <= items_; ++i) {
      zetan_ += 1.0 / std::pow(static_cast<double>(i), theta_);
    }
    const double zeta2 = 1.0 + 1.0 / std::pow(2.0, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    eta_ =
        (1.0 - std::pow(2.0 / static_cast<double>(items_), 1.0 - theta_)) / (1.0 - zeta2 / zetan_);
  }

  uint64_t Next(std::mt19937_64& rng) const {
    const double u = std::uniform_real_distribution<double>(0, 1)(rng);
    const double uz = u * zetan_;
    uint64_t rank = 0;
    if (uz < 1.0) {
      rank = 0;
    } else if (uz < 1.0 + std::pow(0.5, theta_)) {
      rank = 1;
    } else {
      rank = static_cast<uint64_t>(static_cast<double>(items_) *
                                   std::pow(eta_ * u - eta_ + 1.0, alpha_));
    }
    return Fnv64(rank) % items_;
  }

 private:
  static uint64_t Fnv64(uint64_t v) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (int i = 0; i < 8; ++i) {
      h ^= (v >> (8 * i)) & 0xff;
      h *= 0x100000001b3ULL;
    }
    return h;
  }

  uint64_t items_;
  double theta_;
  double zetan_ = 0;
  double alpha_ = 0;
  double eta_ = 0;
};

}  // namespace kv::bench
