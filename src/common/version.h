#pragma once

#include <string_view>

namespace kv {

// Version reports the build version baked in at compile time.
std::string_view Version();

}  // namespace kv
