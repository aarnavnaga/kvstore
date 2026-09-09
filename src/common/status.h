#pragma once

#include <string>
#include <string_view>
#include <utility>

namespace kv {

// Status carries the outcome of an operation that can fail. A default-constructed Status
// is OK; every other code carries a human-readable message.
class Status {
 public:
  enum class Code : unsigned char {
    kOk = 0,
    kNotFound,
    kCorruption,
    kIOError,
    kInvalidArgument,
    kNotLeader,
    kTimeout,
    kUnavailable,
  };

  Status() = default;

  static Status OK() { return {}; }
  static Status NotFound(std::string_view msg = {}) { return {Code::kNotFound, msg}; }
  static Status Corruption(std::string_view msg) { return {Code::kCorruption, msg}; }
  static Status IOError(std::string_view msg) { return {Code::kIOError, msg}; }
  static Status InvalidArgument(std::string_view msg) { return {Code::kInvalidArgument, msg}; }
  static Status NotLeader(std::string_view msg = {}) { return {Code::kNotLeader, msg}; }
  static Status Timeout(std::string_view msg = {}) { return {Code::kTimeout, msg}; }
  static Status Unavailable(std::string_view msg) { return {Code::kUnavailable, msg}; }

  // IOErrorFromErrno builds an IOError from the current errno, prefixed with context.
  static Status IOErrorFromErrno(std::string_view context);

  [[nodiscard]] bool ok() const { return code_ == Code::kOk; }
  [[nodiscard]] bool IsNotFound() const { return code_ == Code::kNotFound; }
  [[nodiscard]] bool IsCorruption() const { return code_ == Code::kCorruption; }
  [[nodiscard]] bool IsNotLeader() const { return code_ == Code::kNotLeader; }
  [[nodiscard]] Code code() const { return code_; }
  [[nodiscard]] const std::string& message() const { return msg_; }
  [[nodiscard]] std::string ToString() const;

 private:
  Status(Code code, std::string_view msg) : code_(code), msg_(msg) {}

  Code code_ = Code::kOk;
  std::string msg_;
};

}  // namespace kv

// KV_RETURN_IF_ERROR propagates a non-OK Status to the caller.
#define KV_RETURN_IF_ERROR(expr)      \
  do {                                \
    ::kv::Status _kv_status = (expr); \
    if (!_kv_status.ok()) {           \
      return _kv_status;              \
    }                                 \
  } while (false)
