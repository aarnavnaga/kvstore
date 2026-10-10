#pragma once

// Generates throwaway CAs and certificates in-process (OpenSSL 3), so TLS tests need no
// fixtures on disk and no openssl CLI.

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <cstdlib>
#include <memory>
#include <string>

#include "net/tls.h"

namespace kv::test {

struct Pem {
  std::string cert;
  std::string key;
};

namespace detail {

inline std::string ToPem(X509* x) {
  BIO* b = BIO_new(BIO_s_mem());
  PEM_write_bio_X509(b, x);
  char* data = nullptr;
  const long n = BIO_get_mem_data(b, &data);
  std::string out(data, static_cast<size_t>(n));
  BIO_free(b);
  return out;
}

inline std::string ToPem(EVP_PKEY* k) {
  BIO* b = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(b, k, nullptr, nullptr, 0, nullptr, nullptr);
  char* data = nullptr;
  const long n = BIO_get_mem_data(b, &data);
  std::string out(data, static_cast<size_t>(n));
  BIO_free(b);
  return out;
}

inline void AddExt(X509* cert, X509* issuer, int nid, const char* value) {
  X509V3_CTX ctx;
  X509V3_set_ctx(&ctx, issuer, cert, nullptr, nullptr, 0);
  X509_EXTENSION* ext = X509V3_EXT_conf_nid(nullptr, &ctx, nid, value);
  X509_add_ext(cert, ext, -1);
  X509_EXTENSION_free(ext);
}

inline X509* NewCert(const std::string& cn, EVP_PKEY* key, long serial) {
  X509* x = X509_new();
  X509_set_version(x, 2);
  ASN1_INTEGER_set(X509_get_serialNumber(x), serial);
  X509_gmtime_adj(X509_getm_notBefore(x), -60);
  X509_gmtime_adj(X509_getm_notAfter(x), 24 * 3600);
  X509_set_pubkey(x, key);
  X509_NAME* name = X509_get_subject_name(x);
  X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                             reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
  return x;
}

}  // namespace detail

inline Pem MakeCa(const std::string& cn) {
  EVP_PKEY* key = EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "P-256");
  X509* x = detail::NewCert(cn, key, 1);
  X509_set_issuer_name(x, X509_get_subject_name(x));
  detail::AddExt(x, x, NID_basic_constraints, "critical,CA:TRUE");
  detail::AddExt(x, x, NID_key_usage, "critical,keyCertSign,cRLSign");
  X509_sign(x, key, EVP_sha256());
  Pem out{detail::ToPem(x), detail::ToPem(key)};
  X509_free(x);
  EVP_PKEY_free(key);
  return out;
}

inline Pem Issue(const Pem& ca, const std::string& cn) {
  BIO* cb = BIO_new_mem_buf(ca.cert.data(), static_cast<int>(ca.cert.size()));
  X509* ca_cert = PEM_read_bio_X509(cb, nullptr, nullptr, nullptr);
  BIO_free(cb);
  BIO* kb = BIO_new_mem_buf(ca.key.data(), static_cast<int>(ca.key.size()));
  EVP_PKEY* ca_key = PEM_read_bio_PrivateKey(kb, nullptr, nullptr, nullptr);
  BIO_free(kb);

  EVP_PKEY* key = EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "P-256");
  static long serial = 100;
  X509* x = detail::NewCert(cn, key, ++serial);
  X509_set_issuer_name(x, X509_get_subject_name(ca_cert));
  detail::AddExt(x, ca_cert, NID_basic_constraints, "critical,CA:FALSE");
  detail::AddExt(x, ca_cert, NID_ext_key_usage, "serverAuth,clientAuth");
  X509_sign(x, ca_key, EVP_sha256());
  Pem out{detail::ToPem(x), detail::ToPem(key)};
  X509_free(x);
  EVP_PKEY_free(key);
  X509_free(ca_cert);
  EVP_PKEY_free(ca_key);
  return out;
}

// TlsFor returns a context holding a certificate issued by ca and trusting ca.
inline std::shared_ptr<net::TlsContext> TlsFor(const Pem& ca, const std::string& cn) {
  const Pem leaf = Issue(ca, cn);
  std::shared_ptr<net::TlsContext> ctx;
  if (!net::TlsContext::CreateFromPem(ca.cert, leaf.cert, leaf.key, &ctx).ok()) {
    std::abort();
  }
  return ctx;
}

}  // namespace kv::test
