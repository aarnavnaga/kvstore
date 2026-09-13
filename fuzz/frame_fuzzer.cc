// Feeds arbitrary bytes to the frame decoder, split at an input-chosen point, and parses
// every decoded frame as a request and as a response. The server runs exactly this code
// on bytes from untrusted clients.

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string_view>

#include "net/frame.h"
#include "net/protocol.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0) {
    return 0;
  }
  const std::string_view in(reinterpret_cast<const char*>(data) + 1, size - 1);
  const size_t split = in.empty() ? 0 : data[0] % (in.size() + 1);

  kv::net::FrameDecoder decoder(1 << 16);
  decoder.Feed(in.substr(0, split));
  decoder.Feed(in.substr(split));
  kv::net::Frame frame;
  while (decoder.Next(&frame) == kv::net::FrameDecoder::Result::kFrame) {
    kv::net::KvRequest req;
    if (kv::net::DecodeKvRequest(frame.type, frame.payload, &req)) {
      // Decoding must be stable: re-encoding a decoded request and decoding it again
      // yields the same fields. (Not the same bytes: a varint has non-minimal encodings,
      // e.g. 0x80 0x00 is also zero.)
      kv::net::KvRequest again;
      if (!kv::net::DecodeKvRequest(frame.type, kv::net::EncodeKvRequest(req), &again) ||
          again.key != req.key || again.value != req.value || again.client_id != req.client_id ||
          again.seq != req.seq) {
        std::abort();
      }
    }
    kv::net::KvResponse resp;
    (void)kv::net::DecodeKvResponse(frame.payload, &resp);
  }
  return 0;
}
