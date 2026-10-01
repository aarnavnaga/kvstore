#include "net/tls.h"

#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <csignal>
#include <mutex>

#include "common/file.h"

namespace kv::net {

std::string TlsError() {
  std::string out;
  while (const unsigned long e = ERR_get_error()) {
    char buf[256];
    ERR_error_string_n(e, buf, sizeof(buf));
    if (!out.empty()) {
      out += "; ";
    }
    out += buf;
  }
  return out.empty() ? "unknown TLS error" : out;
}

namespace {

// ConfigureContext applies the shared policy: TLS 1.3, our certificate and key, the
// cluster CA as the only trust anchor, and a mandatory peer certificate.
Status ConfigureContext(SSL_CTX* ctx, const std::string& ca_pem, const std::string& cert_pem,
                        const std::string& key_pem) {
  SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
  // Writes may complete partially and be retried from a moved buffer: the event loop
  // keeps its own output buffer and reallocates it.
  SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

  BIO* cert_bio = BIO_new_mem_buf(cert_pem.data(), static_cast<int>(cert_pem.size()));
  X509* cert = PEM_read_bio_X509(cert_bio, nullptr, nullptr, nullptr);
  BIO_free(cert_bio);
  BIO* key_bio = BIO_new_mem_buf(key_pem.data(), static_cast<int>(key_pem.size()));
  EVP_PKEY* key = PEM_read_bio_PrivateKey(key_bio, nullptr, nullptr, nullptr);
  BIO_free(key_bio);
  const bool loaded = cert != nullptr && key != nullptr &&
                      SSL_CTX_use_certificate(ctx, cert) == 1 &&
                      SSL_CTX_use_PrivateKey(ctx, key) == 1 && SSL_CTX_check_private_key(ctx) == 1;
  X509_free(cert);
  EVP_PKEY_free(key);
  if (!loaded) {
    return Status::InvalidArgument("certificate/key: " + TlsError());
  }

  X509_STORE* store = SSL_CTX_get_cert_store(ctx);
  BIO* ca_bio = BIO_new_mem_buf(ca_pem.data(), static_cast<int>(ca_pem.size()));
  int added = 0;
  while (X509* ca = PEM_read_bio_X509(ca_bio, nullptr, nullptr, nullptr)) {
    added += X509_STORE_add_cert(store, ca) == 1 ? 1 : 0;
    X509_free(ca);
  }
  BIO_free(ca_bio);
  ERR_clear_error();  // reading past the last PEM block leaves a benign error queued
  if (added == 0) {
    return Status::InvalidArgument("no CA certificate found");
  }
  SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
  return Status::OK();
}

}  // namespace

Status TlsFromFiles(const std::string& ca, const std::string& cert, const std::string& key,
                    std::shared_ptr<TlsContext>* out) {
  out->reset();
  if (ca.empty() && cert.empty() && key.empty()) {
    return Status::OK();
  }
  if (ca.empty() || cert.empty() || key.empty()) {
    return Status::InvalidArgument("TLS needs all of --tls_ca, --tls_cert and --tls_key");
  }
  return TlsContext::Create(TlsOptions{ca, cert, key}, out);
}

Status TlsContext::Create(const TlsOptions& options, std::shared_ptr<TlsContext>* out) {
  std::string ca;
  std::string cert;
  std::string key;
  KV_RETURN_IF_ERROR(ReadFileToString(options.ca_file, &ca));
  KV_RETURN_IF_ERROR(ReadFileToString(options.cert_file, &cert));
  KV_RETURN_IF_ERROR(ReadFileToString(options.key_file, &key));
  return CreateFromPem(ca, cert, key, out);
}

Status TlsContext::CreateFromPem(const std::string& ca_pem, const std::string& cert_pem,
                                 const std::string& key_pem, std::shared_ptr<TlsContext>* out) {
  // OpenSSL's socket BIO writes with write(2), which cannot take MSG_NOSIGNAL. Writing
  // to a connection the peer just closed would raise SIGPIPE and, by default, kill the
  // whole process. Turn it into an EPIPE error instead (bug log #13).
  static std::once_flag ignore_sigpipe;
  std::call_once(ignore_sigpipe, [] { std::signal(SIGPIPE, SIG_IGN); });
  std::shared_ptr<TlsContext> ctx(new TlsContext());
  KV_RETURN_IF_ERROR(ctx->Init(ca_pem, cert_pem, key_pem));
  *out = std::move(ctx);
  return Status::OK();
}

Status TlsContext::Init(const std::string& ca_pem, const std::string& cert_pem,
                        const std::string& key_pem) {
  server_ = SSL_CTX_new(TLS_server_method());
  client_ = SSL_CTX_new(TLS_client_method());
  if (server_ == nullptr || client_ == nullptr) {
    return Status::IOError("SSL_CTX_new: " + TlsError());
  }
  KV_RETURN_IF_ERROR(ConfigureContext(server_, ca_pem, cert_pem, key_pem));
  return ConfigureContext(client_, ca_pem, cert_pem, key_pem);
}

TlsContext::~TlsContext() {
  SSL_CTX_free(server_);
  SSL_CTX_free(client_);
}

SSL* TlsContext::NewServer(int fd) const {
  SSL* ssl = SSL_new(server_);
  SSL_set_fd(ssl, fd);
  SSL_set_accept_state(ssl);
  return ssl;
}

SSL* TlsContext::NewClient(int fd) const {
  SSL* ssl = SSL_new(client_);
  SSL_set_fd(ssl, fd);
  SSL_set_connect_state(ssl);
  return ssl;
}

}  // namespace kv::net
