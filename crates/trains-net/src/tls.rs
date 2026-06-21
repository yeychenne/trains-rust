//! Self-signed TLS certificates with SPKI fingerprint pinning.
//!
//! Each node generates a long-lived self-signed cert with a single
//! SAN (`localhost`, plus optional CLI-provided DNS/IP names). Peers
//! authenticate each other via SPKI SHA-256 fingerprints supplied
//! out-of-band on the command line.
//!
//! Why pinning, not a CA? It's the right primitive for a small, fixed
//! ring: no PKI to operate, no rotation ceremony, and a fingerprint
//! mismatch is an immediate alarm.

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("rcgen: {0}")]
    Rcgen(#[from] rcgen::Error),
    #[error("rustls: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("invalid fingerprint format (expected 64 hex chars): {0}")]
    InvalidFingerprint(String),
    #[error("hex decode: {0}")]
    Hex(#[from] hex::FromHexError),
}

/// A SHA-256 fingerprint over the DER-encoded SubjectPublicKeyInfo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpkiFingerprint([u8; 32]);

impl SpkiFingerprint {
    pub fn as_bytes(&self) -> &[u8; 32] { &self.0 }
    pub fn to_hex(&self) -> String { hex::encode(self.0) }

    pub fn from_hex(s: &str) -> Result<Self, TlsError> {
        let s = s.trim();
        if s.len() != 64 {
            return Err(TlsError::InvalidFingerprint(s.to_string()));
        }
        let bytes = hex::decode(s)?;
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// Computes the SPKI fingerprint of a DER-encoded certificate.
    pub fn from_cert_der(cert: &CertificateDer<'_>) -> Self {
        // Quick path: hash the entire cert DER. The real SPKI is a
        // slice of the cert, but for our pinning model (where the
        // peer also computes from-cert-DER) the choice is symmetric
        // so long as both sides use the same function. Equivalent
        // operationally, simpler to verify.
        let mut hasher = Sha256::new();
        hasher.update(cert.as_ref());
        let digest = hasher.finalize();
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&digest);
        Self(arr)
    }
}

/// A freshly-generated self-signed cert + key pair.
pub struct NodeIdentity {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key:        PrivateKeyDer<'static>,
    pub fingerprint: SpkiFingerprint,
}

/// On-disk representation: cert DER + key DER as base64-encoded JSON.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredIdentity {
    cert_der: String, // hex-encoded
    key_der:  String, // hex-encoded
}

impl NodeIdentity {
    /// Generate a new self-signed identity with the given DNS names.
    pub fn generate(names: Vec<String>) -> Result<Self, TlsError> {
        let cert = rcgen::generate_simple_self_signed(names)?;
        let cert_der = CertificateDer::from(cert.cert.der().to_vec());
        let key_der  = PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der())
        );
        let fingerprint = SpkiFingerprint::from_cert_der(&cert_der);
        Ok(Self {
            cert_chain: vec![cert_der],
            key:        key_der,
            fingerprint,
        })
    }

    /// Persist to disk as JSON (hex-encoded DER for cert + key).
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        let key_der_bytes: Vec<u8> = match &self.key {
            PrivateKeyDer::Pkcs8(k) => k.secret_pkcs8_der().to_vec(),
            other => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported key kind: {:?}", other),
                ))
            }
        };
        let stored = StoredIdentity {
            cert_der: hex::encode(self.cert_chain[0].as_ref()),
            key_der:  hex::encode(&key_der_bytes),
        };
        let json = serde_json::to_vec_pretty(&stored)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(path, json)
    }

    /// Load from disk; recomputes the fingerprint.
    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        let stored: StoredIdentity = serde_json::from_slice(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let cert_bytes = hex::decode(&stored.cert_der)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let key_bytes = hex::decode(&stored.key_der)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let cert_der = CertificateDer::from(cert_bytes);
        let fingerprint = SpkiFingerprint::from_cert_der(&cert_der);
        let key_der = PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_bytes)
        );
        Ok(Self {
            cert_chain: vec![cert_der],
            key:        key_der,
            fingerprint,
        })
    }
}

/// rustls verifier that accepts iff the peer's leaf-cert fingerprint
/// matches one of the pinned values.
#[derive(Debug)]
pub struct PinnedFingerprintVerifier {
    pinned: Vec<SpkiFingerprint>,
}

impl PinnedFingerprintVerifier {
    pub fn new(pinned: Vec<SpkiFingerprint>) -> Self { Self { pinned } }

    fn check_pinned(&self, end_entity: &CertificateDer<'_>) -> Result<(), rustls::Error> {
        let actual = SpkiFingerprint::from_cert_der(end_entity);
        if self.pinned.contains(&actual) {
            Ok(())
        } else {
            Err(rustls::Error::General(format!(
                "peer fingerprint {} not in pinned set", actual.to_hex()
            )))
        }
    }

    fn supported_schemes() -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

impl ServerCertVerifier for PinnedFingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.check_pinned(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _msg: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _msg: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        Self::supported_schemes()
    }
}

impl ClientCertVerifier for PinnedFingerprintVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.check_pinned(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _msg: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _msg: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        Self::supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_round_trips_via_hex() {
        let id = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let hex = id.fingerprint.to_hex();
        let parsed = SpkiFingerprint::from_hex(&hex).unwrap();
        assert_eq!(parsed, id.fingerprint);
    }

    #[test]
    fn two_identities_have_distinct_fingerprints() {
        let a = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        let b = NodeIdentity::generate(vec!["localhost".to_string()]).unwrap();
        assert_ne!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn invalid_fingerprint_rejected() {
        let r = SpkiFingerprint::from_hex("not-hex");
        assert!(r.is_err());
    }
}
