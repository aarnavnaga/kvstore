#include "server/standalone_service.h"

#include "net/protocol.h"

namespace kv::server {

void StandaloneService::Handle(const net::Frame& request, const net::Responder& respond) {
  net::Frame response;
  response.type = net::MsgType::kResponse;

  net::KvResponse resp;
  switch (request.type) {
    case net::MsgType::kPing:
      break;
    case net::MsgType::kStatus: {
      const storage::DBStats st = db_->GetStats();
      response.type = net::MsgType::kStatus;
      response.payload = "role=standalone tables=" + std::to_string(st.num_tables) +
                         " writes=" + std::to_string(st.writes);
      respond(std::move(response));
      return;
    }
    case net::MsgType::kPut:
    case net::MsgType::kGet:
    case net::MsgType::kDelete: {
      net::KvRequest req;
      if (!net::DecodeKvRequest(request.type, request.payload, &req)) {
        resp = net::ResponseFromStatus(Status::InvalidArgument("malformed request"));
        break;
      }
      Status s;
      if (req.op == net::MsgType::kPut) {
        s = db_->Put(req.key, req.value);
      } else if (req.op == net::MsgType::kDelete) {
        s = db_->Delete(req.key);
      } else {
        s = db_->Get(req.key, &resp.value);
      }
      if (!s.ok()) {
        resp = net::ResponseFromStatus(s);
      }
      break;
    }
    default:
      resp = net::ResponseFromStatus(Status::InvalidArgument("unsupported message type"));
      break;
  }
  response.payload = net::EncodeKvResponse(resp);
  respond(std::move(response));
}

}  // namespace kv::server
