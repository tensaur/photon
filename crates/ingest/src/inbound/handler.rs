use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::{Run, RunStatus};
use photon_core::types::ack::{AckResult, AckStatus};
use photon_core::types::batch::WireBatch;
use photon_core::types::error::ApiError;
use photon_core::types::event::PhotonEvent;
use photon_core::types::id::RunId;
use photon_core::types::ingest::{IngestMessage, IngestResult};
use photon_store::ports::{ReadRepository, WriteRepository};
use photon_transport::ports::{Transport, TransportError};
use tokio::sync::broadcast;

use crate::domain::service::IngestService;

/// Map an ingest message to a result using the given service and entity writers.
pub async fn dispatch<S, W, E, P>(
    service: &S,
    run_writer: &W,
    experiment_writer: &E,
    project_writer: &P,
    finished_runs_tx: &tokio::sync::mpsc::UnboundedSender<RunId>,
    event_tx: &broadcast::Sender<PhotonEvent>,
    msg: IngestMessage,
) -> IngestResult
where
    S: IngestService,
    W: ReadRepository<Run> + WriteRepository<Run>,
    E: WriteRepository<Experiment>,
    P: WriteRepository<Project>,
{
    match msg {
        IngestMessage::Batch(batch) => {
            let ack = ingest_batch(service, &batch).await;
            IngestResult::Ack(ack)
        }
        IngestMessage::RegisterRun(run) => match run_writer.upsert(&run).await {
            Ok(()) => {
                let _ = event_tx.send(PhotonEvent::RunStatusChanged {
                    run_id: run.id(),
                    old: run.status().clone(),
                    new: run.status().clone(),
                });
                IngestResult::RunRegistered(run.id())
            }
            Err(e) => {
                tracing::error!("register run failed: {e}");
                IngestResult::Error(ApiError::Internal)
            }
        },
        IngestMessage::FinishRun(run_id) => {
            finish_run(run_writer, finished_runs_tx, event_tx, run_id).await
        }
        IngestMessage::RegisterExperiment(experiment) => {
            match experiment_writer.upsert(&experiment).await {
                Ok(()) => IngestResult::ExperimentRegistered(experiment.id),
                Err(e) => {
                    tracing::error!("register experiment failed: {e}");
                    IngestResult::Error(ApiError::Internal)
                }
            }
        }
        IngestMessage::RegisterProject(project) => match project_writer.upsert(&project).await {
            Ok(()) => IngestResult::ProjectRegistered(project.id),
            Err(e) => {
                tracing::error!("register project failed: {e}");
                IngestResult::Error(ApiError::Internal)
            }
        },
        IngestMessage::QueryWatermark(run_id) => match service.watermark(&run_id).await {
            Ok(seq) => IngestResult::Watermark(seq),
            Err(e) => {
                tracing::error!("watermark query failed: {e}");
                IngestResult::Error(ApiError::Internal)
            }
        },
    }
}

async fn finish_run<W>(
    run_writer: &W,
    finished_runs_tx: &tokio::sync::mpsc::UnboundedSender<RunId>,
    event_tx: &broadcast::Sender<PhotonEvent>,
    run_id: RunId,
) -> IngestResult
where
    W: ReadRepository<Run> + WriteRepository<Run>,
{
    let Some(mut run) = (match run_writer.get(&run_id).await {
        Ok(run) => run,
        Err(e) => {
            tracing::error!("load run for finish failed: {e}");
            return IngestResult::Error(ApiError::Internal);
        }
    }) else {
        return IngestResult::Error(ApiError::NotFound {
            message: format!("run {run_id} not found"),
        });
    };

    if matches!(run.status(), RunStatus::Running) {
        if let Err(e) = run.finish() {
            tracing::error!("finish run transition failed: {e}");
            return IngestResult::Error(ApiError::Internal);
        }

        if let Err(e) = run_writer.upsert(&run).await {
            tracing::error!("persist finished run failed: {e}");
            return IngestResult::Error(ApiError::Internal);
        }

        let _ = event_tx.send(PhotonEvent::RunStatusChanged {
            run_id,
            old: RunStatus::Running,
            new: RunStatus::Finished,
        });
    }

    if finished_runs_tx.send(run_id).is_err() {
        tracing::error!("persist finish_run notification failed: receiver dropped");
        return IngestResult::Error(ApiError::Internal);
    }

    IngestResult::RunFinished(run_id)
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
    service: &S,
    run_writer: &W,
    experiment_writer: &E,
    project_writer: &P,
    finished_runs_tx: &tokio::sync::mpsc::UnboundedSender<RunId>,
    event_tx: &broadcast::Sender<PhotonEvent>,
    transport: &T,
) where
    S: IngestService,
    W: ReadRepository<Run> + WriteRepository<Run>,
    E: WriteRepository<Experiment>,
    P: WriteRepository<Project>,
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
            service,
            run_writer,
            experiment_writer,
            project_writer,
            finished_runs_tx,
            event_tx,
            msg,
        )
        .await;

        if transport.send(&result).await.is_err() {
            break;
        }
    }
}
