//! Trace identifiers.

use std::fmt;

/// A 16-byte W3C / OTLP trace id.
///
/// The all-zero id is invalid per the OTLP specification and is rejected by
/// [`TraceId::from_slice`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TraceId(pub [u8; 16]);

impl TraceId {
    /// Parse a trace id from raw OTLP bytes. Returns `None` when the slice is
    /// not exactly 16 bytes or is all zeroes.
    pub fn from_slice(bytes: &[u8]) -> Option<Self> {
        let arr: [u8; 16] = bytes.try_into().ok()?;
        if arr == [0u8; 16] {
            return None;
        }
        Some(TraceId(arr))
    }

    /// Build a trace id from a 128-bit integer (big-endian byte order).
    pub fn from_u128(v: u128) -> Self {
        TraceId(v.to_be_bytes())
    }

    /// The id as a 128-bit integer (big-endian byte order).
    pub fn as_u128(&self) -> u128 {
        u128::from_be_bytes(self.0)
    }

    /// Lower-case hex representation (32 characters).
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(32);
        for b in self.0 {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({})", self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_bad_lengths_and_zero() {
        assert!(TraceId::from_slice(&[1u8; 15]).is_none());
        assert!(TraceId::from_slice(&[1u8; 17]).is_none());
        assert!(TraceId::from_slice(&[0u8; 16]).is_none());
        assert!(TraceId::from_slice(&[7u8; 16]).is_some());
    }

    #[test]
    fn hex_roundtrip_shape() {
        let id = TraceId::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert_eq!(id.to_hex(), "0102030405060708090a0b0c0d0e0f10");
        assert_eq!(id.to_string(), id.to_hex());
        assert_eq!(id.as_u128(), 0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert!(format!("{id:?}").starts_with("TraceId("));
    }
}
