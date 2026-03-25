use std::sync::Arc;

use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::batch::WireBatch;
use photon_core::types::ingest::{IngestMessage, IngestResult};
use photon_store::ports::experiment::ExperimentWriter;
use photon_store::ports::project::ProjectWriter;
use photon_store::ports::run::RunWriter;
use photon_transport::ports::{Transport, TransportError};

use crate::domain::service::IngestService;

/// Map an ingest message to a result using the given service, run writer, experiment writer, and project writer.
pub async fn dispatch<S: IngestService, W: RunWriter, E: ExperimentWriter, P: ProjectWriter>(
    service: &S,
    run_writer: &W,
    experiment_writer: &E,
    project_writer: &P,
    msg: IngestMessage,
) -> IngestResult {
    match msg {
        IngestMessage::Batch(batch) => {
            let ack = ingest_batch(service, &batch).await;
            IngestResult::Ack(ack)
        }
        IngestMessage::RegisterRun(run) => match run_writer.upsert_run(&run).await {
            Ok(()) => IngestResult::RunRegistered(run.id),
            Err(e) => IngestResult::Error(e.to_string()),
        },
        IngestMessage::RegisterExperiment(experiment) => {
            match experiment_writer.upsert_experiment(&experiment).await {
                Ok(()) => IngestResult::ExperimentRegistered(experiment.id),
                Err(e) => IngestResult::Error(e.to_string()),
            }
        }
        IngestMessage::RegisterProject(project) => {
            match project_writer.upsert_project(&project).await {
                Ok(()) => IngestResult::ProjectRegistered(project.id),
                Err(e) => IngestResult::Error(e.to_string()),
            }
        }
        IngestMessage::QueryWatermark(run_id) => match service.watermark(&run_id).await {
            Ok(seq) => IngestResult::Watermark(seq),
            Err(e) => IngestResult::Error(e.to_string()),
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

/// Transport-agnostic ingest handler using envelope types.
pub async fn handle_envelope<S, W, E, P, T>(
    service: &Arc<S>,
    run_writer: &W,
    experiment_writer: &E,
    project_writer: &P,
    transport: &T,
) where
    S: IngestService,
    W: RunWriter,
    E: ExperimentWriter,
    P: ProjectWriter,
    T: Transport<IngestResult, IngestMessage>,
{
    loop {
        let msg = match transport.recv().await {
            Ok(msg) => msg,
            Err(TransportError::StreamClosed(_)) => break,
            Err(e) => {
                tracing::warn!("ingest transport error: {e}");
                break;
            }
        };

        let result = dispatch(
            &**service,
            run_writer,
            experiment_writer,
            project_writer,
            msg,
        )
        .await;

        if transport.send(&result).await.is_err() {
            break;
        }
    }
}
