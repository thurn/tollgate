use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

use crate::DomainError;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObjectFormat {
    Sha1,
    Sha256,
}

impl ObjectFormat {
    pub const fn byte_len(self) -> usize {
        match self {
            Self::Sha1 => 20,
            Self::Sha256 => 32,
        }
    }
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GitOid {
    pub format: ObjectFormat,
    #[serde(with = "oid_bytes")]
    bytes: Vec<u8>,
}

impl GitOid {
    pub fn new(format: ObjectFormat, bytes: Vec<u8>) -> Result<Self, DomainError> {
        if bytes.len() != format.byte_len() {
            return Err(DomainError::InvalidOidLength {
                expected: format.byte_len(),
                actual: bytes.len(),
            });
        }
        Ok(Self { format, bytes })
    }

    pub fn from_hex(value: &str) -> Result<Self, DomainError> {
        let format = match value.len() {
            40 => ObjectFormat::Sha1,
            64 => ObjectFormat::Sha256,
            actual => {
                return Err(DomainError::InvalidOidLength {
                    expected: 40,
                    actual,
                });
            }
        };
        let bytes = hex::decode(value).map_err(|_| DomainError::InvalidOid(value.to_owned()))?;
        Self::new(format, bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.bytes)
    }

    pub fn short(&self) -> String {
        self.to_hex()[..10].to_owned()
    }
}

impl fmt::Debug for GitOid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GitOid").field(&self.to_hex()).finish()
    }
}

impl fmt::Display for GitOid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for GitOid {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::from_hex(value)
    }
}

mod oid_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        hex::decode(value).map_err(serde::de::Error::custom)
    }
}
