use crate::error::TransportError;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Frames larger than this are rejected outright rather than allocated —
/// a defensive bound against a corrupt or adversarial length prefix, since
/// every frame on every category (including attachment chunks, which are
/// deliberately sized well under this) is bounded far below it in practice.
const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Writes `bytes` as one length-prefixed frame: a 4-byte big-endian length
/// followed by the bytes themselves. Generic over `AsyncWrite` so it works
/// identically against `iroh::endpoint::SendStream` and plain in-memory
/// buffers, as the tests below exercise.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    bytes: &[u8],
) -> Result<(), TransportError> {
    let len = u32::try_from(bytes.len()).map_err(|_| {
        TransportError::Codec("frame too large to encode a length prefix".to_string())
    })?;
    w.write_all(&len.to_be_bytes())
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    w.write_all(bytes)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(())
}

/// Reads back one frame written by `write_frame`. Returns
/// `Err(TransportError::Codec(_))` if the length prefix exceeds
/// `MAX_FRAME_LEN`, and `Err(TransportError::Io(_))` if the stream ends
/// before the declared length is fully read — both cases a malformed or
/// adversarial peer could trigger, so neither may panic.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Vec<u8>, TransportError> {
    let mut len_bytes = [0u8; 4];
    r.read_exact(&mut len_bytes)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    let len = u32::from_be_bytes(len_bytes);
    if len > MAX_FRAME_LEN {
        return Err(TransportError::Codec(format!(
            "frame length {len} exceeds MAX_FRAME_LEN {MAX_FRAME_LEN}"
        )));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)
        .await
        .map_err(|e| TransportError::Io(e.to_string()))?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn a_written_frame_reads_back_identical() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"hello frame").await.unwrap();

        let mut cursor = Cursor::new(buf);
        let read_back = read_frame(&mut cursor).await.unwrap();
        assert_eq!(read_back, b"hello frame".to_vec());
    }

    #[tokio::test]
    async fn read_frame_rejects_a_length_prefix_over_the_max_frame_size() {
        // A hand-crafted length prefix claiming an absurd frame size --
        // must be rejected before attempting to allocate/read that many
        // bytes, since this could be attacker-supplied.
        let mut buf: Vec<u8> = (u32::MAX).to_be_bytes().to_vec();
        let mut cursor = Cursor::new(buf.split_off(0));
        let result = read_frame(&mut cursor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_frame_returns_err_on_truncated_input() {
        // A length prefix promising 100 bytes, but the stream ends after 3.
        let mut buf = 100u32.to_be_bytes().to_vec();
        buf.extend_from_slice(b"abc");
        let mut cursor = Cursor::new(buf);
        assert!(read_frame(&mut cursor).await.is_err());
    }

    #[tokio::test]
    async fn an_empty_frame_round_trips() {
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, b"").await.unwrap();

        let mut cursor = Cursor::new(buf);
        let read_back = read_frame(&mut cursor).await.unwrap();
        assert_eq!(read_back, Vec::<u8>::new());
    }

    #[tokio::test]
    async fn a_large_frame_round_trips() {
        // Well under MAX_FRAME_LEN, but large enough to exercise more than
        // a single internal read/write call.
        let payload = vec![0xAB_u8; 1024 * 1024];
        let mut buf: Vec<u8> = Vec::new();
        write_frame(&mut buf, &payload).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let read_back = read_frame(&mut cursor).await.unwrap();
        assert_eq!(read_back, payload);
    }
}
