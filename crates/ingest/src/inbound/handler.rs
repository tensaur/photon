use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::batch::WireBatch;
use photon_core::types::error::ApiError;
use photon_core::types::ingest::{IngestMessage, IngestResult};
use photon_transport::ports::{Transport, TransportError};

use crate::domain::service::IngestService;

/// Map an ingest message to a result using the domain service.
pub async fn dispatch<S: IngestService>(service: &S, msg: IngestMessage) -> IngestResult {
    match msg {
        IngestMessage::Batch(batch) => {
            let ack = ingest_batch(service, &batch).await;
            IngestResult::Ack(ack)
        }
        IngestMessage::RegisterRun(run) => match service.register_run(&run).await {
            Ok(()) => IngestResult::RunRegistered(run.id()),
            Err(e) => {
                tracing::error!("register run failed: {e}");
                IngestResult::Error(ApiError::Internal)
            }
        },
        IngestMessage::FinishRun(run_id) => match service.finish_run(run_id).await {
            Ok(()) => IngestResult::RunFinished(run_id),
            Err(e) => {
                tracing::error!("finish run failed: {e}");
                IngestResult::Error(ApiError::Internal)
            }
        },
        IngestMessage::RegisterExperiment(experiment) => {
            match service.register_experiment(&experiment).await {
                Ok(()) => IngestResult::ExperimentRegistered(experiment.id),
                Err(e) => {
                    tracing::error!("register experiment failed: {e}");
                    IngestResult::Error(ApiError::Internal)
                }
            }
        }
        IngestMessage::RegisterProject(project) => {
            match service.register_project(&project).await {
                Ok(()) => IngestResult::ProjectRegistered(project.id),
                Err(e) => {
                    tracing::error!("register project failed: {e}");
                    IngestResult::Error(ApiError::Internal)
                }
            }
        }
        IngestMessage::QueryWatermark(run_id) => match service.watermark(&run_id).await {
            Ok(seq) => IngestResult::Watermark(seq),
            Err(e) => {
                tracing::error!("watermark query failed: {e}");
                IngestResult::Error(ApiError::Internal)
            }
        },
    }
}

/// Process a single wire batch, returning an ack.
async fn ingest_batch<S: IngestService>(service: &S, batch: &WireBatch) -> AckResult {
    match service.ingest(batch).await {
        Ok(result) => AckResult {
            sequence_number: result.sequence_number,
            status: result.status,
        },
        Err(e) => {
            tracing::error!("ingest error: {e}");
            AckResult {
                sequence_number: batch.sequence_number,
                status: AckStatus::Rejected,
            }
        }
    }
}

/// Transport-agnostic ingest handler.
pub async fn handle_envelope<S: IngestService, T: Transport<IngestResult, IngestMessage>>(
    service: &S,
    transport: &T,
) {
    loop {
        let msg = match transport.recv().await {
            Ok(msg) => msg,
            Err(TransportError::StreamClosed(_)) => break,
            Err(e) => {
                tracing::warn!("ingest transport error: {e}");
                break;
            }
        };

        let result = dispatch(service, msg).await;

        if transport.send(&result).await.is_err() {
            break;
        }
    }
}
