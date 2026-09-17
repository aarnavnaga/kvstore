// kvcli: command-line client.
//
//   kvcli [--servers=host:port,...] put <key> <value>
//   kvcli [--servers=...] get <key>
//   kvcli [--servers=...] del <key>
//   kvcli [--servers=...] ping
//   kvcli [--servers=...] status

#include <cstdio>
#include <iostream>

#include "client/kv_client.h"
#include "common/flags.h"

namespace {

int Usage() {
  std::fprintf(stderr,
               "usage: kvcli [--servers=host:port,...] [--timeout_ms=N] "
               "put <key> <value> | get <key> | del <key> | ping | status\n");
  return 2;
}

int Fail(const kv::Status& s) {
  std::fprintf(stderr, "error: %s\n", s.ToString().c_str());
  return s.IsNotFound() ? 1 : 3;
}

}  // namespace

int main(int argc, char** argv) {
  const kv::Flags flags(argc, argv);
  const auto& args = flags.positional();
  if (args.empty()) {
    return Usage();
  }
  kv::client::ClientOptions options;
  options.servers = kv::SplitList(flags.Get("servers", "127.0.0.1:7000"));
  options.operation_timeout = kv::net::Millis(flags.GetInt("timeout_ms", 10000));
  kv::client::KvClient client(options);

  const std::string& cmd = args[0];
  if (cmd == "put" && args.size() == 3) {
    const kv::Status s = client.Put(args[1], args[2]);
    if (!s.ok()) {
      return Fail(s);
    }
    std::puts("OK");
  } else if (cmd == "get" && args.size() == 2) {
    std::string value;
    const kv::Status s = client.Get(args[1], &value);
    if (!s.ok()) {
      return Fail(s);
    }
    std::cout << value << "\n";
  } else if ((cmd == "del" || cmd == "delete") && args.size() == 2) {
    const kv::Status s = client.Delete(args[1]);
    if (!s.ok()) {
      return Fail(s);
    }
    std::puts("OK");
  } else if (cmd == "ping" && args.size() == 1) {
    for (const auto& addr : options.servers) {
      const kv::Status s = client.Ping(addr);
      std::printf("%s %s\n", addr.c_str(), s.ok() ? "OK" : s.ToString().c_str());
    }
  } else if (cmd == "status" && args.size() == 1) {
    for (const auto& addr : options.servers) {
      std::string out;
      const kv::Status s = client.NodeStatus(addr, &out);
      std::printf("%s %s\n", addr.c_str(), s.ok() ? out.c_str() : s.ToString().c_str());
    }
  } else {
    return Usage();
  }
  return 0;
}
