#include "common/status.h"

#include <cerrno>
#include <system_error>

namespace kv {

Status Status::IOErrorFromErrno(std::string_view context) {
  std::string msg(context);
  msg += ": ";
  // error_code::message is thread-safe, unlike strerror.
  msg += std::error_code(errno, std::generic_category()).message();
  return IOError(msg);
}

std::string Status::ToString() const {
  const char* name = "OK";
  switch (code_) {
    case Code::kOk:
      return "OK";
    case Code::kNotFound:
      name = "NotFound";
      break;
    case Code::kCorruption:
      name = "Corruption";
      break;
    case Code::kIOError:
      name = "IOError";
      break;
    case Code::kInvalidArgument:
      name = "InvalidArgument";
      break;
    case Code::kNotLeader:
      name = "NotLeader";
      break;
    case Code::kTimeout:
      name = "Timeout";
      break;
    case Code::kUnavailable:
      name = "Unavailable";
      break;
  }
  std::string out(name);
  if (!msg_.empty()) {
    out += ": ";
    out += msg_;
  }
  return out;
}

}  // namespace kv
