use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tokio::sync::oneshot;

use lasso::ThreadedRodeo;

use photon_batch::run_batch_thread;
use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::{Run as DomainRun, RunStatus};
use photon_core::types::config::{BatchConfig, UplinkConfig};
use photon_core::types::id::{ExperimentId, ProjectId, RunId, TenantId, UserId};
use photon_core::types::sequence::SequenceNumber;
use photon_core::types::wal::WalOffset;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::CompressorKind;
use photon_transport::TransportKind;
use photon_transport::codec::CodecTransport;
use photon_uplink::{TransportError as UplinkTransportError, UplinkThreadError, run_uplink};
use photon_wal::{DiskWalConfig, Wal, WalKind};

use crate::accumulator::Accumulator;
use crate::error::StartError;
use crate::run::{Run, UplinkHandle};

/// Configure and start a [`Run`].
///
/// All fields have defaults. The simplest usage is:
/// ```ignore
/// let mut run = Run::builder().start()?;
/// ```
pub struct RunBuilder {
    run_id: Option<RunId>,
    name: Option<String>,
    project: Option<Project>,
    experiment: Option<Experiment>,
    user_id: Option<UserId>,
    tags: Vec<String>,
    wal_dir: Option<PathBuf>,
    wal: WalKind,
    channel_capacity: usize,
    batch: BatchConfig,
    endpoint: Option<String>,
    transport: TransportKind,
    codec: CodecKind,
    compressor: CompressorKind,
}

impl Default for RunBuilder {
    fn default() -> Self {
        Self {
            run_id: None,
            name: None,
            project: None,
            experiment: None,
            user_id: None,
            tags: Vec::new(),
            wal_dir: None,
            wal: WalKind::default(),
            channel_capacity: 65_536,
            batch: BatchConfig::default(),
            endpoint: None,
            transport: TransportKind::default(),
            codec: CodecKind::default(),
            compressor: CompressorKind::default(),
        }
    }
}

impl RunBuilder {
    pub fn run_id(mut self, id: RunId) -> Self {
        self.run_id = Some(id);
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn project(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        let tenant_id = TenantId::default();
        let id = ProjectId::from_name(&tenant_id, &name);
        let now = chrono::Utc::now();
        self.project = Some(Project {
            id,
            tenant_id,
            name,
            created_at: now,
            updated_at: now,
        });
        self
    }

    pub fn experiment(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        let project_id = self.project.as_ref().map(|p| p.id).unwrap_or_default();
        let id = ExperimentId::from_name(&project_id, &name);
        let now = chrono::Utc::now();
        self.experiment = Some(Experiment {
            id,
            project_id,
            name,
            tags: Vec::new(),
            created_at: now,
            updated_at: now,
        });
        self
    }

    pub fn user(mut self, id: UserId) -> Self {
        self.user_id = Some(id);
        self
    }

    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn wal_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.wal_dir = Some(dir.into());
        self
    }

    pub fn wal(mut self, wal: WalKind) -> Self {
        self.wal = wal;
        self
    }

    pub fn max_points_per_batch(mut self, n: usize) -> Self {
        self.batch.max_points = n;
        self
    }

    pub fn batch_interval(mut self, interval: Duration) -> Self {
        self.batch.batch_interval = interval;
        self
    }

    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    pub fn codec(mut self, codec: CodecKind) -> Self {
        self.codec = codec;
        self
    }

    pub fn compressor(mut self, compressor: CompressorKind) -> Self {
        self.compressor = compressor;
        self
    }

    pub fn transport(mut self, transport: TransportKind) -> Self {
        self.transport = transport;
        self
    }

    pub fn endpoint(mut self, url: impl Into<String>) -> Self {
        self.endpoint = Some(url.into());
        self
    }

    pub fn start(self) -> Result<Run, StartError> {
        let run_id = self.run_id.unwrap_or_default();
        let now = chrono::Utc::now();

        let project = self.project.unwrap_or_else(|| {
            let tenant_id = TenantId::default();
            let name = "default".to_string();
            let id = ProjectId::from_name(&tenant_id, &name);
            Project {
                id,
                tenant_id,
                name,
                created_at: now,
                updated_at: now,
            }
        });

        let domain_run = DomainRun {
            id: run_id,
            project_id: project.id,
            experiment_id: self.experiment.as_ref().map(|e| e.id),
            user_id: self.user_id.unwrap_or_default(),
            name: self
                .name
                .unwrap_or_else(|| format!("run-{}", run_id.short())),
            status: RunStatus::Running,
            tags: self.tags,
            created_at: now,
            updated_at: now,
        };
        let experiment = self.experiment;

        let wal_dir = self
            .wal_dir
            .unwrap_or_else(|| photon_wal::default_wal_dir().join(run_id.to_string()));
        let (wal_appender, wal) = self.wal.open(wal_dir, DiskWalConfig::default())?;

        let interner = Arc::new(ThreadedRodeo::default());
        let (accumulator, rx) = Accumulator::new(self.channel_capacity);

        let start_sequence = wal
            .read_from(WalOffset::ZERO)
            .ok()
            .and_then(|batches: Vec<_>| batches.last().map(|b| b.sequence_number.next()))
            .unwrap_or_else(|| SequenceNumber::ZERO.next());

        let (batch_tx, batch_rx) = crossbeam_channel::bounded(64);

        let codec = self.codec;
        let batch_interner = Arc::clone(&interner);
        let batch_handle = thread::Builder::new()
            .name(format!("photon-batch-{run_id}"))
            .spawn(move || {
                run_batch_thread(
                    run_id,
                    batch_interner,
                    codec,
                    self.compressor,
                    wal_appender,
                    start_sequence,
                    rx,
                    batch_tx,
                    self.batch,
                )
            })
            .expect("failed to spawn batch thread");

        let uplink_handle = self.endpoint.map(|endpoint| {
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let uplink_wal = wal.clone();
            let uplink_config = UplinkConfig::default();
            let transport_kind = self.transport;
            let run = domain_run.clone();
            let project = project.clone();
            let experiment = experiment.clone();

            let handle = thread::Builder::new()
                .name(format!("photon-uplink-{run_id}"))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(UplinkThreadError::Runtime)?;

                    rt.block_on(async {
                        let bt = transport_kind
                            .connect(&endpoint)
                            .await
                            .map_err(UplinkTransportError::from)?;

                        let transport = CodecTransport::new(codec, bt);

                        run_uplink(
                            transport,
                            run,
                            project,
                            experiment,
                            uplink_wal,
                            uplink_config,
                            start_sequence,
                            shutdown_rx,
                            batch_rx,
                        )
                        .await
                    })
                })
                .expect("failed to spawn uplink thread");

            UplinkHandle {
                shutdown_tx: Some(shutdown_tx),
                handle,
            }
        });

        Ok(Run::new(
            run_id,
            accumulator,
            interner,
            batch_handle,
            uplink_handle,
            wal,
        ))
    }
}
