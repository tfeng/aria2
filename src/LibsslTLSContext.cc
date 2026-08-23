/* <!-- copyright */
/*
 * aria2 - The high speed download utility
 *
 * Copyright (C) 2006 Tatsuhiro Tsujikawa
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of portions of this program with the
 * OpenSSL library under certain conditions as described in each
 * individual source file, and distribute linked combinations
 * including the two.
 * You must obey the GNU General Public License in all respects
 * for all of the code used other than OpenSSL.  If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so.  If you
 * do not wish to do so, delete this exception statement from your
 * version.  If you delete this exception statement from all source
 * files in the program, then also delete it here.
 */
/* copyright --> */
#include "LibsslTLSContext.h"

#include <cassert>
#include <sstream>

#include <openssl/err.h>
#include <openssl/pkcs12.h>
#include <openssl/bio.h>

#ifdef HAVE_BROTLI_IMPERSONATE
#  include <brotli/decode.h>
#endif // HAVE_BROTLI_IMPERSONATE

#include "LogFactory.h"
#include "Logger.h"
#include "fmt.h"
#include "message.h"
#include "BufferedFile.h"

namespace {
struct bio_deleter {
  void operator()(BIO* b)
  {
    if (b)
      BIO_free(b);
  }
};
typedef std::unique_ptr<BIO, bio_deleter> bio_t;
struct p12_deleter {
  void operator()(PKCS12* p)
  {
    if (p)
      PKCS12_free(p);
  }
};
typedef std::unique_ptr<PKCS12, p12_deleter> p12_t;
struct pkey_deleter {
  void operator()(EVP_PKEY* x)
  {
    if (x)
      EVP_PKEY_free(x);
  }
};
typedef std::unique_ptr<EVP_PKEY, pkey_deleter> pkey_t;
struct x509_deleter {
  void operator()(X509* x)
  {
    if (x)
      X509_free(x);
  }
};
typedef std::unique_ptr<X509, x509_deleter> x509_t;
struct x509_sk_deleter {
  void operator()(STACK_OF(X509) * x)
  {
    if (x)
      sk_X509_pop_free(x, X509_free);
  }
};
typedef std::unique_ptr<STACK_OF(X509), x509_sk_deleter> x509_sk_t;

#ifdef HAVE_BORINGSSL_IMPERSONATE
// Chrome 116.0.5845.180's exact cipher list, verbatim from curl-impersonate's curl_chrome116 wrapper
// script (see FXPlayer's Docs/SSL.md). Passed to SSL_CTX_set_cipher_list as one combined string —
// BoringSSL accepts the TLS_*-named TLS 1.3 suites within this classic cipher-list string (its TLS 1.3
// suite set is otherwise fixed/automatic); this mirrors curl-impersonate's own call exactly rather than
// splitting it, since that's the proven-working form (its own code never calls the separate
// SSL_CTX_set_ciphersuites path for this profile either — see curl-8.1.1/lib/vtls/openssl.c:3895-3904).
const char* const kChrome116CipherList =
    "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:"
    "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:"
    "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:"
    "ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:"
    "ECDHE-RSA-AES128-SHA:ECDHE-RSA-AES256-SHA:"
    "AES128-GCM-SHA256:AES256-GCM-SHA384:AES128-SHA:AES256-SHA";

#  ifdef HAVE_BROTLI_IMPERSONATE
// Taken from curl-impersonate (adapted from Chromium's net/ssl/cert_compression.cc), which in turn is
// what real Chrome does: registers brotli as a supported certificate-compression algorithm (RFC 8879,
// TLS extension 27) so it appears in our ClientHello the same way it does in Chrome's. Only the
// decompress direction is implemented — like curl-impersonate, we're a client compressing nothing of our
// own; a server that never uses this extension is unaffected either way.
int decompressBrotliCert(SSL*, CRYPTO_BUFFER** out, size_t uncompressedLen,
                         const uint8_t* in, size_t inLen)
{
  uint8_t* data;
  CRYPTO_BUFFER* decompressed = CRYPTO_BUFFER_alloc(&data, uncompressedLen);
  if (!decompressed) {
    return 0;
  }
  size_t outputSize = uncompressedLen;
  if (BrotliDecoderDecompress(inLen, in, &outputSize, data) !=
          BROTLI_DECODER_RESULT_SUCCESS ||
      outputSize != uncompressedLen) {
    CRYPTO_BUFFER_free(decompressed);
    return 0;
  }
  *out = decompressed;
  return 1;
}
#  endif // HAVE_BROTLI_IMPERSONATE
#endif   // HAVE_BORINGSSL_IMPERSONATE

} // namespace

namespace aria2 {

TLSContext* TLSContext::make(TLSSessionSide side, TLSVersion minVer)
{
  return new OpenSSLTLSContext(side, minVer);
}

OpenSSLTLSContext::OpenSSLTLSContext(TLSSessionSide side, TLSVersion minVer)
    : sslCtx_(nullptr), side_(side), verifyPeer_(true)
{
  sslCtx_ = SSL_CTX_new(SSLv23_method());
  if (sslCtx_) {
    good_ = true;
  }
  else {
    good_ = false;
    A2_LOG_ERROR(fmt("SSL_CTX_new() failed. Cause: %s",
                     ERR_error_string(ERR_get_error(), nullptr)));
    return;
  }

  long ver_opts = 0;
  switch (minVer) {
#ifdef TLS1_3_VERSION
  case TLS_PROTO_TLS13:
    ver_opts |= SSL_OP_NO_TLSv1_2;
    // fall through
#endif // TLS1_3_VERSION
  case TLS_PROTO_TLS12:
    ver_opts |= SSL_OP_NO_TLSv1_1;
  // fall through
  case TLS_PROTO_TLS11:
    ver_opts |= SSL_OP_NO_TLSv1;
    ver_opts |= SSL_OP_NO_SSLv3;
    break;
  default:
    assert(0);
    abort();
  };

  // Disable SSLv2 and enable all workarounds for buggy servers
  SSL_CTX_set_options(sslCtx_, SSL_OP_ALL | SSL_OP_NO_SSLv2 | ver_opts
#ifdef SSL_OP_SINGLE_ECDH_USE
                                   | SSL_OP_SINGLE_ECDH_USE
#endif // SSL_OP_SINGLE_ECDH_USE
#ifdef SSL_OP_NO_COMPRESSION
                                   | SSL_OP_NO_COMPRESSION
#endif // SSL_OP_NO_COMPRESSION
  );
  SSL_CTX_set_mode(sslCtx_, SSL_MODE_AUTO_RETRY);
  SSL_CTX_set_mode(sslCtx_, SSL_MODE_ENABLE_PARTIAL_WRITE);
#ifdef SSL_MODE_RELEASE_BUFFERS
  /* keep memory usage low */
  SSL_CTX_set_mode(sslCtx_, SSL_MODE_RELEASE_BUFFERS);
#endif
#ifdef HAVE_BORINGSSL_IMPERSONATE
  // Chrome 116 TLS fingerprint (see Docs/SSL.md for the full investigation + curl-impersonate reference).
  // Client-side only: side_ isn't set until after this block runs (see below), but TLS_SERVER only ever
  // happens for the RPC listener, which has nothing to impersonate — harmless to apply unconditionally.
  if (SSL_CTX_set_cipher_list(sslCtx_, kChrome116CipherList) == 0) {
    good_ = false;
    A2_LOG_ERROR(fmt("SSL_CTX_set_cipher_list() failed. Cause: %s",
                     ERR_error_string(ERR_get_error(), nullptr)));
  }
  // curl-impersonate: extension order permutation, matching Chrome's own randomization
  // (curl-8.1.1/lib/vtls/openssl.c:4003-4004).
  SSL_CTX_set_permute_extensions(sslCtx_, 1);
  // curl-impersonate: TLS extensions 5 (status_request) and 18 (signed_certificate_timestamp) — Chrome
  // sends both on every ClientHello (curl-8.1.1/lib/vtls/openssl.c:3878-3881).
  SSL_CTX_enable_ocsp_stapling(sslCtx_);
  SSL_CTX_enable_signed_cert_timestamps(sslCtx_);
  // curl-impersonate: matches Chrome's own record-splitting + TLS False Start behavior
  // (curl-8.1.1/lib/vtls/openssl.c:3875-3876).
  SSL_CTX_set_mode(sslCtx_, SSL_MODE_CBC_RECORD_SPLITTING | SSL_MODE_ENABLE_FALSE_START);
#  ifdef HAVE_BROTLI_IMPERSONATE
  // curl-impersonate: TLS extension 27 (compress_certificate), brotli algorithm
  // (curl-8.1.1/lib/vtls/openssl.c:2792-2821).
  if (!SSL_CTX_add_cert_compression_alg(sslCtx_, TLSEXT_cert_compression_brotli,
                                        nullptr, decompressBrotliCert)) {
    A2_LOG_WARN("SSL_CTX_add_cert_compression_alg(brotli) failed — continuing without it.");
  }
#  endif // HAVE_BROTLI_IMPERSONATE
  // ALPN: http/1.1 only, NOT h2 — deliberate deviation from curl-impersonate's own reference (which
  // offers h2) documented in Docs/SSL.md: aria2 has no HTTP/2 wire-protocol implementation at all, so
  // advertising h2 and having a server actually pick it would break every download outright. A real
  // Chrome ClientHello always offers h2 too, so this one extension's content remains a known, deliberate
  // gap in the resulting fingerprint — everything else in this profile still applies.
  {
    static const uint8_t kAlpnHttp11[] = {8, 'h', 't', 't', 'p', '/', '1', '.', '1'};
    if (SSL_CTX_set_alpn_protos(sslCtx_, kAlpnHttp11, sizeof(kAlpnHttp11)) != 0) {
      A2_LOG_WARN("SSL_CTX_set_alpn_protos() failed — continuing without ALPN.");
    }
  }
#else    // !HAVE_BORINGSSL_IMPERSONATE
  if (SSL_CTX_set_cipher_list(sslCtx_, "HIGH:!aNULL:!eNULL") == 0) {
    good_ = false;
    A2_LOG_ERROR(fmt("SSL_CTX_set_cipher_list() failed. Cause: %s",
                     ERR_error_string(ERR_get_error(), nullptr)));
  }
#endif   // HAVE_BORINGSSL_IMPERSONATE

#if OPENSSL_VERSION_NUMBER < 0x30000000L &&                                    \
    OPENSSL_VERSION_NUMBER >= 0x0090800fL
#  ifndef OPENSSL_NO_ECDH
  auto ecdh = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
  if (ecdh == nullptr) {
    A2_LOG_WARN(fmt("Failed to enable ECDHE cipher suites. Cause: %s",
                    ERR_error_string(ERR_get_error(), nullptr)));
  }
  else {
    SSL_CTX_set_tmp_ecdh(sslCtx_, ecdh);
    EC_KEY_free(ecdh);
  }
#  endif // OPENSSL_NO_ECDH
#endif   // OPENSSL_VERSION_NUMBER < 0x30000000L && OPENSSL_VERSION_NUMBER >=
         // 0x0090800fL
}

OpenSSLTLSContext::~OpenSSLTLSContext() { SSL_CTX_free(sslCtx_); }

bool OpenSSLTLSContext::good() const { return good_; }

bool OpenSSLTLSContext::addCredentialFile(const std::string& certfile,
                                          const std::string& keyfile)
{
  if (keyfile.empty()) {
    return addP12CredentialFile(certfile);
  }
  if (SSL_CTX_use_PrivateKey_file(sslCtx_, keyfile.c_str(), SSL_FILETYPE_PEM) !=
      1) {
    A2_LOG_ERROR(fmt("Failed to load private key from %s. Cause: %s",
                     keyfile.c_str(),
                     ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }
  if (SSL_CTX_use_certificate_chain_file(sslCtx_, certfile.c_str()) != 1) {
    A2_LOG_ERROR(fmt("Failed to load certificate from %s. Cause: %s",
                     certfile.c_str(),
                     ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }
  A2_LOG_INFO(fmt("Credential files(cert=%s, key=%s) were successfully added.",
                  certfile.c_str(), keyfile.c_str()));
  return true;
}
bool OpenSSLTLSContext::addP12CredentialFile(const std::string& p12file)
{
  std::stringstream ss;
  BufferedFile(p12file.c_str(), BufferedFile::READ).transfer(ss);

  auto data = ss.str();
  void* ptr = const_cast<char*>(data.c_str());
  size_t len = data.length();
  bio_t bio(BIO_new_mem_buf(ptr, len));

  if (!bio) {
    A2_LOG_ERROR("Failed to open PKCS12 file: no memory.");
    return false;
  }
  p12_t p12(d2i_PKCS12_bio(bio.get(), nullptr));
  if (!p12) {
    if (side_ == TLS_SERVER) {
      A2_LOG_ERROR(fmt("Failed to open PKCS12 file: %s. "
                       "If you meant to use PEM, you'll also have to specify "
                       "--rpc-private-key. See the manual.",
                       ERR_error_string(ERR_get_error(), nullptr)));
    }
    else {
      A2_LOG_ERROR(fmt("Failed to open PKCS12 file: %s. "
                       "If you meant to use PEM, you'll also have to specify "
                       "--private-key. See the manual.",
                       ERR_error_string(ERR_get_error(), nullptr)));
    }
    return false;
  }
  EVP_PKEY* pkey;
  X509* cert;
  STACK_OF(X509)* ca = nullptr;
  if (!PKCS12_parse(p12.get(), "", &pkey, &cert, &ca)) {
    if (side_ == TLS_SERVER) {
      A2_LOG_ERROR(fmt("Failed to parse PKCS12 file: %s. "
                       "If you meant to use PEM, you'll also have to specify "
                       "--rpc-private-key. See the manual.",
                       ERR_error_string(ERR_get_error(), nullptr)));
    }
    else {
      A2_LOG_ERROR(fmt("Failed to parse PKCS12 file: %s. "
                       "If you meant to use PEM, you'll also have to specify "
                       "--private-key. See the manual.",
                       ERR_error_string(ERR_get_error(), nullptr)));
    }
    return false;
  }

  pkey_t pkey_holder(pkey);
  x509_t cert_holder(cert);
  x509_sk_t ca_holder(ca);

  if (!pkey || !cert) {
    A2_LOG_ERROR(fmt("Failed to use PKCS12 file: no pkey or cert %s",
                     ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }
  if (!SSL_CTX_use_PrivateKey(sslCtx_, pkey)) {
    A2_LOG_ERROR(fmt("Failed to use PKCS12 file pkey: %s",
                     ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }
  if (!SSL_CTX_use_certificate(sslCtx_, cert)) {
    A2_LOG_ERROR(fmt("Failed to use PKCS12 file cert: %s",
                     ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }
  // SSL_CTX_add_extra_chain_cert takes one X509* (never a stack) in BoringSSL, unlike the OpenSSL API this
  // was originally written against. SSL_CTX_set1_chain takes the whole STACK_OF(X509) directly and is
  // present in both libraries; "set1" (vs. "set0") duplicates each cert's ref rather than taking ownership,
  // which is what's needed here since `ca_holder` frees `ca` itself when this function returns.
  if (ca && sk_X509_num(ca) && !SSL_CTX_set1_chain(sslCtx_, ca)) {
    A2_LOG_ERROR(fmt("Failed to use PKCS12 file chain: %s",
                     ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }

  A2_LOG_INFO("Using certificate and key from PKCS12 file");
  return true;
}

bool OpenSSLTLSContext::addSystemTrustedCACerts()
{
  if (SSL_CTX_set_default_verify_paths(sslCtx_) != 1) {
    A2_LOG_INFO(fmt(MSG_LOADING_SYSTEM_TRUSTED_CA_CERTS_FAILED,
                    ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }
  else {
    A2_LOG_INFO("System trusted CA certificates were successfully added.");
    return true;
  }
}

bool OpenSSLTLSContext::addTrustedCACertFile(const std::string& certfile)
{
  if (SSL_CTX_load_verify_locations(sslCtx_, certfile.c_str(), nullptr) != 1) {
    A2_LOG_ERROR(fmt(MSG_LOADING_TRUSTED_CA_CERT_FAILED, certfile.c_str(),
                     ERR_error_string(ERR_get_error(), nullptr)));
    return false;
  }
  else {
    A2_LOG_INFO("Trusted CA certificates were successfully added.");
    return true;
  }
}

} // namespace aria2
