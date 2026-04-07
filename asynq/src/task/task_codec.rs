use crate::error::Result;
use serde::{de::DeserializeOwned, Serialize};

#[cfg(feature = "json")]
const CONTENT_TYPE_JSON: &str = "application/json";
#[cfg(feature = "msgpack")]
const CONTENT_TYPE_MSGPACK: &str = "application/msgpack";

/// Generic task payload codec.
pub trait PayloadCodec {
  /// Content-Type represented by this codec.
  fn content_type(&self) -> &'static str;

  /// Encode a serializable value to bytes.
  fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>>;

  /// Decode bytes into the target type.
  fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T>;
}

/// JSON payload codec.
#[cfg(feature = "json")]
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonPayloadCodec;

#[cfg(feature = "json")]
impl PayloadCodec for JsonPayloadCodec {
  fn content_type(&self) -> &'static str {
    CONTENT_TYPE_JSON
  }

  fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| crate::error::Error::Serialization(e.to_string()))
  }

  fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| crate::error::Error::Deserialization(e.to_string()))
  }
}

/// MsgPack payload codec.
#[cfg(feature = "msgpack")]
#[derive(Debug, Clone, Copy, Default)]
pub struct MsgPackPayloadCodec;

#[cfg(feature = "msgpack")]
impl PayloadCodec for MsgPackPayloadCodec {
  fn content_type(&self) -> &'static str {
    CONTENT_TYPE_MSGPACK
  }

  fn encode<T: Serialize>(&self, value: &T) -> Result<Vec<u8>> {
    use rmp_serde::Serializer;

    let mut payload = Vec::new();
    value
      .serialize(&mut Serializer::new(&mut payload))
      .map_err(|e| crate::error::Error::Serialization(e.to_string()))?;
    Ok(payload)
  }

  fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
    rmp_serde::from_slice(bytes).map_err(|e| crate::error::Error::Deserialization(e.to_string()))
  }
}
