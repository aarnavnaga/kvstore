#include "net/frame.h"

#include "common/coding.h"

namespace kv::net {

bool IsValidMsgType(uint8_t t) {
  return t >= static_cast<uint8_t>(MsgType::kPing) && t <= static_cast<uint8_t>(MsgType::kStatus);
}

void EncodeFrame(const Frame& frame, std::string* out) {
  PutFixed32(out, static_cast<uint32_t>(1 + 8 + frame.payload.size()));
  out->push_back(static_cast<char>(frame.type));
  PutFixed64(out, frame.request_id);
  out->append(frame.payload);
}

std::string EncodeFrame(const Frame& frame) {
  std::string out;
  out.reserve(kFrameHeaderSize + frame.payload.size());
  EncodeFrame(frame, &out);
  return out;
}

void FrameDecoder::Feed(std::string_view data) {
  // Compact once the consumed prefix dominates, so a long-lived connection's buffer
  // does not grow without bound.
  if (pos_ > 0 && pos_ >= buf_.size() / 2) {
    buf_.erase(0, pos_);
    pos_ = 0;
  }
  buf_.append(data);
}

FrameDecoder::Result FrameDecoder::Next(Frame* frame) {
  if (!error_.empty()) {
    return Result::kError;
  }
  const size_t avail = buf_.size() - pos_;
  if (avail < 4) {
    return Result::kNeedMore;
  }
  const uint32_t length = DecodeFixed32(buf_.data() + pos_);
  if (length < 1 + 8) {
    error_ = "frame length " + std::to_string(length) + " below header size";
    return Result::kError;
  }
  if (length > max_frame_size_) {
    // Checked before buffering the body, so a hostile length cannot make us allocate.
    error_ = "frame length " + std::to_string(length) + " exceeds limit";
    return Result::kError;
  }
  if (avail - 4 < length) {
    return Result::kNeedMore;
  }
  const char* p = buf_.data() + pos_ + 4;
  const auto type = static_cast<uint8_t>(p[0]);
  if (!IsValidMsgType(type)) {
    error_ = "unknown message type " + std::to_string(type);
    return Result::kError;
  }
  frame->type = static_cast<MsgType>(type);
  frame->request_id = DecodeFixed64(p + 1);
  frame->payload.assign(p + 9, length - 9);
  pos_ += 4 + length;
  if (pos_ == buf_.size()) {
    buf_.clear();
    pos_ = 0;
  }
  return Result::kFrame;
}

}  // namespace kv::net
