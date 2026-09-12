#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace kv {

// Flags is a minimal command-line parser for "--name=value", "--name value" and bare
// "--name" (a boolean). Everything that is not a flag is a positional argument.
class Flags {
 public:
  Flags(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
      std::string arg = argv[i];
      if (arg.rfind("--", 0) != 0 || arg.size() == 2) {
        positional_.push_back(arg);
        continue;
      }
      arg.erase(0, 2);
      const size_t eq = arg.find('=');
      if (eq != std::string::npos) {
        values_[arg.substr(0, eq)] = arg.substr(eq + 1);
      } else if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0) {
        values_[arg] = argv[++i];
      } else {
        values_[arg] = "true";
      }
    }
  }

  [[nodiscard]] bool Has(const std::string& name) const { return values_.count(name) > 0; }

  [[nodiscard]] std::string Get(const std::string& name, const std::string& fallback = "") const {
    const auto it = values_.find(name);
    return it == values_.end() ? fallback : it->second;
  }

  [[nodiscard]] int64_t GetInt(const std::string& name, int64_t fallback) const {
    const auto it = values_.find(name);
    return it == values_.end() ? fallback : std::stoll(it->second);
  }

  [[nodiscard]] bool GetBool(const std::string& name, bool fallback) const {
    const auto it = values_.find(name);
    if (it == values_.end()) {
      return fallback;
    }
    return it->second == "true" || it->second == "1" || it->second == "yes";
  }

  [[nodiscard]] const std::vector<std::string>& positional() const { return positional_; }

 private:
  std::map<std::string, std::string> values_;
  std::vector<std::string> positional_;
};

// SplitList splits "a,b,c" into its non-empty parts.
inline std::vector<std::string> SplitList(const std::string& s, char sep = ',') {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= s.size()) {
    const size_t end = s.find(sep, start);
    const std::string part =
        s.substr(start, end == std::string::npos ? std::string::npos : end - start);
    if (!part.empty()) {
      out.push_back(part);
    }
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return out;
}

}  // namespace kv
