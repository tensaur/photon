use std::sync::Arc;

use bytes::BytesMut;
use photon_protocol::ports::codec::Codec;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

use crate::ports::{Transport, TransportError};

/// TCP transport adapter using length-prefixed frames.
pub struct TcpTransport<C> {
    codec: C,
    write: Arc<Mutex<BufWriter<OwnedWriteHalf>>>,
    read: Arc<Mutex<BufReader<OwnedReadHalf>>>,
}

impl<C: Clone> Clone for TcpTransport<C> {
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            write: Arc::clone(&self.write),
            read: Arc::clone(&self.read),
        }
    }
}

impl<C> TcpTransport<C> {
    /// Connect to a remote endpoint.
    pub async fn connect(addr: &str, codec: C) -> Result<Self, TransportError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| TransportError::Connection(e.to_string()))?;
        Ok(Self::from_stream(stream, codec))
    }

    /// Wrap an already-accepted TCP stream.
    pub fn from_stream(stream: TcpStream, codec: C) -> Self {
        stream.set_nodelay(true).ok();
        let (read, write) = stream.into_split();
        Self {
            codec,
            write: Arc::new(Mutex::new(BufWriter::new(write))),
            read: Arc::new(Mutex::new(BufReader::new(read))),
        }
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

        let mut write = self.write.lock().await;
        write
            .write_all(&len)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;
        write
            .write_all(&buf)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;
        write
            .flush()
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;

        Ok(())
    }

    async fn recv(&self) -> Result<R, TransportError> {
        let mut read = self.read.lock().await;

        let mut len_buf = [0u8; 4];
        read.read_exact(&mut len_buf)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        read.read_exact(&mut payload)
            .await
            .map_err(|e| TransportError::StreamClosed(e.to_string()))?;

        Codec::<R>::decode(&self.codec, &payload)
            .map_err(|e| TransportError::Request(e.to_string()))
    }
}
