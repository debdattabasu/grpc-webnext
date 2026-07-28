//! gRPC message framing: `[1-byte compression flag][u32 big-endian length][message]`.
//!
//! This is the whole of the wire format on this path, because everything else is
//! HTTP/2's job. Compression is not implemented; a compressed frame is rejected
//! rather than mis-read, since silently handing a caller a compressed blob as if it
//! were a message is the worse failure.

use crate::status::{Code, Status};

/// Frame one message for sending.
pub fn encode_message(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + message.len());
    out.push(0); // not compressed
    out.extend_from_slice(&(message.len() as u32).to_be_bytes());
    out.extend_from_slice(message);
    out
}

/// Reassembles messages from a byte stream that arrives in arbitrary chunks.
#[derive(Default)]
pub struct Deframer {
    buffer: Vec<u8>,
    max_message_bytes: Option<usize>,
}

impl Deframer {
    pub fn new(max_message_bytes: Option<usize>) -> Deframer {
        Deframer { buffer: Vec::new(), max_message_bytes }
    }

    /// Feed a chunk; returns every message that is now complete.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<Vec<u8>>, Status> {
        self.buffer.extend_from_slice(chunk);
        let mut out = Vec::new();
        let mut offset = 0;
        loop {
            if self.buffer.len() - offset < 5 {
                break;
            }
            let flag = self.buffer[offset];
            if flag != 0 {
                return Err(Status::new(
                    Code::Unimplemented,
                    "compressed gRPC messages are not supported by this client",
                ));
            }
            let len = u32::from_be_bytes(
                self.buffer[offset + 1..offset + 5].try_into().expect("5 bytes checked above"),
            ) as usize;
            if let Some(max) = self.max_message_bytes {
                if len > max {
                    return Err(Status::new(
                        Code::ResourceExhausted,
                        format!("message of {len} bytes exceeds the {max}-byte limit"),
                    ));
                }
            }
            if self.buffer.len() - offset - 5 < len {
                break;
            }
            out.push(self.buffer[offset + 5..offset + 5 + len].to_vec());
            offset += 5 + len;
        }
        self.buffer.drain(..offset);
        Ok(out)
    }

    /// Bytes held back awaiting the rest of a message. Non-zero at end of stream
    /// means the body was truncated.
    pub fn pending(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_one_message() {
        let mut d = Deframer::new(None);
        assert_eq!(d.push(&encode_message(b"hello")).unwrap(), vec![b"hello".to_vec()]);
        assert_eq!(d.pending(), 0);
    }

    #[test]
    fn reassembles_across_chunk_boundaries() {
        // The interesting case: HTTP/2 DATA frames split wherever they like, so a
        // header can arrive one byte at a time.
        let framed = encode_message(b"hello");
        let mut d = Deframer::new(None);
        for byte in &framed[..framed.len() - 1] {
            assert!(d.push(&[*byte]).unwrap().is_empty());
        }
        assert_eq!(d.push(&framed[framed.len() - 1..]).unwrap(), vec![b"hello".to_vec()]);
    }

    #[test]
    fn yields_several_messages_from_one_chunk() {
        let mut chunk = encode_message(b"a");
        chunk.extend(encode_message(b"bb"));
        chunk.extend(encode_message(b"ccc"));
        let mut d = Deframer::new(None);
        assert_eq!(
            d.push(&chunk).unwrap(),
            vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]
        );
    }

    #[test]
    fn empty_messages_are_messages() {
        let mut d = Deframer::new(None);
        assert_eq!(d.push(&encode_message(b"")).unwrap(), vec![Vec::<u8>::new()]);
    }

    #[test]
    fn a_partial_message_is_visible_as_pending() {
        let framed = encode_message(b"hello");
        let mut d = Deframer::new(None);
        assert!(d.push(&framed[..7]).unwrap().is_empty());
        assert_eq!(d.pending(), 7); // truncated body, not a clean end
    }

    #[test]
    fn rejects_a_compressed_frame_rather_than_mis_reading_it() {
        let mut framed = encode_message(b"hello");
        framed[0] = 1;
        let err = Deframer::new(None).push(&framed).unwrap_err();
        assert_eq!(err.code, Code::Unimplemented);
    }

    #[test]
    fn enforces_the_size_limit_before_buffering_the_body() {
        // The length prefix is enough to refuse; waiting for the bytes would mean
        // buffering exactly what the limit exists to prevent.
        let mut d = Deframer::new(Some(4));
        let err = d.push(&encode_message(b"too long")).unwrap_err();
        assert_eq!(err.code, Code::ResourceExhausted);
    }
}
