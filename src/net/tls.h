#pragma once

#include <memory>
#include <string>

#include "common/status.h"

using SSL = struct ssl_st;
using SSL_CTX = struct ssl_ctx_st;

namespace kv::net {

struct TlsOptions {
  std::string ca_file;    // the cluster CA; peers must present certificates it signed
  std::string cert_file;  // this process's certificate (PEM)
  std::string key_file;   // and its private key (PEM)
};

// TlsContext holds the OpenSSL configuration for mutual TLS: TLS 1.3 only, and both
// sides must present a certificate signed by the cluster CA. One context serves every
// connection in the process, in both directions; it is safe to share across threads.
//
// Peers are authenticated by CA membership alone (no hostname check): every node and
// operator client holds a certificate from the same private CA, and anything without one
// cannot connect.
class TlsContext {
 public:
  static Status Create(const TlsOptions& options, std::shared_ptr<TlsContext>* out);
  // CreateFromPem is Create with the PEM contents passed directly (tests).
  static Status CreateFromPem(const std::string& ca_pem, const std::string& cert_pem,
                              const std::string& key_pem, std::shared_ptr<TlsContext>* out);
  ~TlsContext();

  TlsContext(const TlsContext&) = delete;
  TlsContext& operator=(const TlsContext&) = delete;

  // NewServer / NewClient create a session bound to a connected socket, in accept or
  // connect state. The caller owns the result (SSL_free).
  [[nodiscard]] SSL* NewServer(int fd) const;
  [[nodiscard]] SSL* NewClient(int fd) const;

 private:
  TlsContext() = default;
  Status Init(const std::string& ca_pem, const std::string& cert_pem, const std::string& key_pem);

  SSL_CTX* server_ = nullptr;
  SSL_CTX* client_ = nullptr;
};

// TlsError drains OpenSSL's thread-local error queue into a readable string.
std::string TlsError();

// TlsFromFiles builds a context when all three paths are set, leaves *out null when none
// are, and fails when only some are (a half-configured TLS setup is a mistake).
Status TlsFromFiles(const std::string& ca, const std::string& cert, const std::string& key,
                    std::shared_ptr<TlsContext>* out);

}  // namespace kv::net
