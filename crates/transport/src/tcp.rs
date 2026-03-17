use std::sync::Arc;

use bytes::BytesMut;
use photon_protocol::ports::codec::Codec;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

use crate::ports::{Transport, TransportError};

struct TcpConnection {
    write: BufWriter<OwnedWriteHalf>,
    read: BufReader<OwnedReadHalf>,
}

/// TCP transport adapter using length-prefixed frames.
pub struct TcpTransport<C> {
    codec: C,
    conn: Arc<Mutex<Option<TcpConnection>>>,
}

impl<C: Clone> Clone for TcpTransport<C> {
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            conn: Arc::clone(&self.conn),
        }
    }
}

impl<C> TcpTransport<C> {
    /// Create an unconnected transport. Call `connect()` before use.
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Wrap an already-accepted TCP stream. Ready to use immediately.
    pub fn from_stream(stream: TcpStream, codec: C) -> Self {
        stream.set_nodelay(true).ok();
        let (read, write) = stream.into_split();

        Self {
            codec,
            conn: Arc::new(Mutex::new(Some(TcpConnection {
                write: BufWriter::new(write),
                read: BufReader::new(read),
            }))),
        }
    }

    /// Establish a TCP connection to the given endpoint.
    pub async fn connect(&self, addr: &str) -> Result<(), TransportError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;

        stream.set_nodelay(true).ok();

        let (read, write) = stream.into_split();
        *self.conn.lock().await = Some(TcpConnection {
            write: BufWriter::new(write),
            read: BufReader::new(read),
        });

        Ok(())
    }
}

impl<S, R, C> Transport<S, R> for TcpTransport<C>
where
    S: Send + Sync + 'static,
    R: Send + 'static,
    C: Codec<S> + Codec<R> + Send + Sync + Clone + 'static,
{
    async fn send(&self, msg: &S) -> Result<(), TransportError> {
        let mut buf = BytesMut::new();
        Codec::<S>::encode(&self.codec, msg, &mut buf)
            .map_err(|e| TransportError::Request(e.to_string()))?;

        let len = (buf.len() as u32).to_be_bytes();

        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| TransportError::Connection("not connected".into()))?;

        conn.write
            .write_all(&len)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;
        conn.write
            .write_all(&buf)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;
        conn.write
            .flush()
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;

        Ok(())
    }

    async fn recv(&self) -> Result<R, TransportError> {
        let mut guard = self.conn.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| TransportError::Connection("not connected".into()))?;

        let mut len_buf = [0u8; 4];
        conn.read
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        conn.read
            .read_exact(&mut payload)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;

        Codec::<R>::decode(&self.codec, &payload)
            .map_err(|e| TransportError::Request(e.to_string()))
    }
}
