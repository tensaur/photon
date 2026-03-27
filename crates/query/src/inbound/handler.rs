use photon_core::types::query::{QueryMessage, QueryResult};
use photon_transport::ports::{Transport, TransportError};

use crate::domain::service::{QueryService, dispatch};

/// Transport-agnostic query handler.
pub async fn handle<S, T>(service: &S, transport: &T)
where
    S: QueryService,
    T: Transport<QueryResult, QueryMessage>,
{
    loop {
        let msg = match transport.recv().await {
            Ok(msg) => msg,
            Err(TransportError::StreamClosed(_)) => break,
            Err(e) => {
                tracing::warn!("query transport error: {e}");
                break;
            }
        };

        let result = dispatch(service, msg).await;

        if transport.send(&result).await.is_err() {
            break;
        }
    }
}
