#pragma once

// Wire framing shared by client traffic and node-to-node Raft traffic:
//
//   fixed32 length      bytes that follow this field (1 + 8 + payload)
//   u8      type        MsgType
//   fixed64 request_id  echoed in the response so clients can pipeline
//   payload
//
// A length prefix (rather than a delimiter) lets the reader allocate once and never scan
// payload bytes, and the request id lets one connection carry many requests in flight.

#include <cstdint>
#include <string>
#include <string_view>

namespace kv::net {

enum class MsgType : uint8_t {
  kPing = 1,
  kPut = 2,
  kGet = 3,
  kDelete = 4,
  kResponse = 5,
  kRaft = 6,    // a Raft RPC between nodes
  kStatus = 7,  // node and cluster status, for operators and health checks
};

bool IsValidMsgType(uint8_t t);

struct Frame {
  MsgType type = MsgType::kPing;
  uint64_t request_id = 0;
  std::string payload;
};

constexpr size_t kFrameHeaderSize = 4 + 1 + 8;
constexpr uint32_t kDefaultMaxFrameSize = 64u << 20;

// EncodeFrame appends the wire encoding of frame to out.
void EncodeFrame(const Frame& frame, std::string* out);
std::string EncodeFrame(const Frame& frame);

// FrameDecoder turns a byte stream into frames. Bytes may arrive split at any point;
// Feed buffers them and Next returns each complete frame.
class FrameDecoder {
 public:
  explicit FrameDecoder(uint32_t max_frame_size = kDefaultMaxFrameSize)
      : max_frame_size_(max_frame_size) {}

  void Feed(std::string_view data);

  enum class Result { kFrame, kNeedMore, kError };
  // Next extracts the next complete frame. After kError the stream is unrecoverable
  // (there is no way to find the next frame boundary) and the connection must close.
  Result Next(Frame* frame);

  [[nodiscard]] const std::string& error() const { return error_; }
  [[nodiscard]] size_t buffered() const { return buf_.size() - pos_; }

 private:
  uint32_t max_frame_size_;
  std::string buf_;
  size_t pos_ = 0;
  std::string error_;
};

}  // namespace kv::net
