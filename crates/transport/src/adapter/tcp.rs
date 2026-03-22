use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

use async_trait::async_trait;

use crate::ports::{ByteTransport, TransportError};

struct TcpConnection {
    write: BufWriter<OwnedWriteHalf>,
    read: BufReader<OwnedReadHalf>,
}

/// TCP transport adapter using length-prefixed frames.
#[derive(Clone)]
pub struct TcpTransport {
    conn: Arc<Mutex<TcpConnection>>,
}

impl TcpTransport {
    /// Wrap an already-accepted TCP stream.
    pub fn accept(stream: TcpStream) -> Self {
        stream.set_nodelay(true).ok();
        let (read, write) = stream.into_split();

        Self {
            conn: Arc::new(Mutex::new(TcpConnection {
                write: BufWriter::new(write),
                read: BufReader::new(read),
            })),
        }
    }

    /// Establish a TCP connection to the given endpoint.
    pub async fn connect(addr: &str) -> Result<Self, TransportError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        stream.set_nodelay(true).ok();
        Ok(Self::accept(stream))
    }
}

#[async_trait]
impl ByteTransport for TcpTransport {
    async fn send_bytes(&self, bytes: &[u8]) -> Result<(), TransportError> {
        let len = (bytes.len() as u32).to_be_bytes();

        let mut guard = self.conn.lock().await;

        guard
            .write
            .write_all(&len)
            .await
            .map_err(TransportError::from_io)?;
        guard
            .write
            .write_all(bytes)
            .await
            .map_err(TransportError::from_io)?;
        guard.write.flush().await.map_err(TransportError::from_io)?;

        Ok(())
    }

    async fn recv_bytes(&self) -> Result<Vec<u8>, TransportError> {
        let mut guard = self.conn.lock().await;

        let mut len_buf = [0u8; 4];
        guard
            .read
            .read_exact(&mut len_buf)
            .await
            .map_err(TransportError::from_io)?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        guard
            .read
            .read_exact(&mut payload)
            .await
            .map_err(TransportError::from_io)?;

        Ok(payload)
    }
}
