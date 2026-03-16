use photon_protocol::codec::CodecChoice;
use photon_transport::{ConnectedTransport, TransportChoice};

use crate::domain::ports::error::TransportError;

/// Connect batch and watermark transports to a server endpoint.
pub async fn connect(
    choice: &TransportChoice,
    endpoint: &str,
    codec: CodecChoice,
) -> Result<(ConnectedTransport, ConnectedTransport), TransportError> {
    let batch = choice
        .connect(endpoint, codec.clone())
        .await
        .map_err(TransportError::from)?;

    let watermark = choice
        .connect(endpoint, codec)
        .await
        .map_err(TransportError::from)?;

    Ok((batch, watermark))
}
