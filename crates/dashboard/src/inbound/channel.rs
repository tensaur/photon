use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{
    MetricQuery, MetricSeries, QueryRequest, QueryResponse,
};
use photon_core::types::id::SubscriptionId;
use photon_core::types::stream::{DeltaData, StreamFrame, SubscriptionCommand, SubscriptionUpdate};
use photon_transport::Transport;

use crate::domain::error::{
    ListExperimentsError, ListMetricsError, ListProjectsError, ListRunsError, QueryMetricsError,
};
use crate::domain::service::DashboardService;

#[derive(Debug)]
pub enum Command {
    ListRuns,
    ListExperiments,
    ListProjects,
    ListMetrics { run_id: RunId },
    Query { query: MetricQuery },
    QueryBatch { request: QueryRequest },
    Subscribe { query: MetricQuery },
    Unsubscribe { subscription_id: SubscriptionId },
    CheckFinalised { run_id: RunId },
}

pub enum Response {
    Runs(Result<Vec<Run>, ListRunsError>),
    Experiments(Result<Vec<Experiment>, ListExperimentsError>),
    Projects(Result<Vec<Project>, ListProjectsError>),
    Metrics {
        run_id: RunId,
        result: Result<Vec<Metric>, ListMetricsError>,
    },
    Series {
        query: MetricQuery,
        result: Result<MetricSeries, QueryMetricsError>,
    },
    BatchSeries {
        request: QueryRequest,
        result: Result<QueryResponse, QueryMetricsError>,
    },
    Snapshot {
        subscription_id: SubscriptionId,
        series: MetricSeries,
    },
    Delta {
        subscription_id: SubscriptionId,
        data: DeltaData,
    },
    Unsubscribed {
        subscription_id: SubscriptionId,
    },
    Finalised {
        run_id: RunId,
    },
    RunsChanged,
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(unreachable_pub)]
mod platform {
    use super::{Command, Response};

    pub type CommandSender = tokio::sync::mpsc::UnboundedSender<Command>;
    pub type CommandReceiver = tokio::sync::mpsc::UnboundedReceiver<Command>;
    pub type ResponseSender = tokio::sync::mpsc::UnboundedSender<Response>;
    pub type ResponseReceiver = tokio::sync::mpsc::UnboundedReceiver<Response>;

    pub fn channels<T>() -> (
        tokio::sync::mpsc::UnboundedSender<T>,
        tokio::sync::mpsc::UnboundedReceiver<T>,
    ) {
        tokio::sync::mpsc::unbounded_channel()
    }

    pub fn send_cmd(tx: &CommandSender, cmd: Command) {
        let _ = tx.send(cmd);
    }
    pub fn send_resp(tx: &ResponseSender, resp: Response) {
        let _ = tx.send(resp);
    }
    pub async fn recv_cmd(rx: &mut CommandReceiver) -> Option<Command> {
        rx.recv().await
    }

    pub fn spawn(fut: impl std::future::Future<Output = ()> + Send + 'static) {
        tokio::spawn(fut);
    }
}

#[cfg(target_arch = "wasm32")]
#[allow(unreachable_pub)]
mod platform {
    use super::*;

    pub type CommandSender = futures_channel::mpsc::UnboundedSender<Command>;
    pub type CommandReceiver = futures_channel::mpsc::UnboundedReceiver<Command>;
    pub type ResponseSender = futures_channel::mpsc::UnboundedSender<Response>;
    pub type ResponseReceiver = futures_channel::mpsc::UnboundedReceiver<Response>;

    pub fn channels<T>() -> (
        futures_channel::mpsc::UnboundedSender<T>,
        futures_channel::mpsc::UnboundedReceiver<T>,
    ) {
        futures_channel::mpsc::unbounded()
    }

    pub fn send_cmd(tx: &CommandSender, cmd: Command) {
        let _ = tx.unbounded_send(cmd);
    }
    pub fn send_resp(tx: &ResponseSender, resp: Response) {
        let _ = tx.unbounded_send(resp);
    }
    pub async fn recv_cmd(rx: &mut CommandReceiver) -> Option<Command> {
        futures_util::StreamExt::next(rx).await
    }

    pub fn spawn(fut: impl std::future::Future<Output = ()> + 'static) {
        wasm_bindgen_futures::spawn_local(fut);
    }
}

pub(crate) use platform::{CommandSender, ResponseReceiver, send_cmd};
use platform::{channels, recv_cmd, send_resp, spawn};
type ResponseSender = platform::ResponseSender;

pub(crate) fn spawn_service<S, T>(
    ctx: egui::Context,
    service: S,
    subscription_transport: Option<T>,
) -> (CommandSender, ResponseReceiver)
where
    S: DashboardService,
    T: Transport<SubscriptionCommand, StreamFrame> + 'static,
{
    let (cmd_tx, cmd_rx) = channels();
    let (resp_tx, resp_rx) = channels();

    if let Some(sub_transport) = subscription_transport {
        let tx = resp_tx.clone();
        let c = ctx.clone();
        spawn(async move { subscription_reader(c, sub_transport, tx).await });
    }

    spawn(run_loop(ctx, service, cmd_rx, resp_tx));
    (cmd_tx, resp_rx)
}

async fn subscription_reader<T>(ctx: egui::Context, transport: T, resp_tx: ResponseSender)
where
    T: Transport<SubscriptionCommand, StreamFrame>,
{
    loop {
        match transport.recv().await {
            Ok(StreamFrame::Subscription { id, update }) => {
                let resp = match update {
                    SubscriptionUpdate::Snapshot { series } => Response::Snapshot {
                        subscription_id: id,
                        series,
                    },
                    SubscriptionUpdate::Delta(data) => Response::Delta {
                        subscription_id: id,
                        data,
                    },
                    SubscriptionUpdate::Unsubscribed => Response::Unsubscribed {
                        subscription_id: id,
                    },
                };
                send_resp(&resp_tx, resp);
                ctx.request_repaint();
            }
            Ok(StreamFrame::RunFinalised { run_id }) => {
                send_resp(&resp_tx, Response::Finalised { run_id });
                ctx.request_repaint();
            }
            Ok(StreamFrame::RunsChanged) => {
                send_resp(&resp_tx, Response::RunsChanged);
                ctx.request_repaint();
            }
            Err(_) => break,
        }
    }
}

async fn run_loop<S: DashboardService>(
    ctx: egui::Context,
    service: S,
    mut cmd_rx: platform::CommandReceiver,
    resp_tx: ResponseSender,
) {
    while let Some(cmd) = recv_cmd(&mut cmd_rx).await {
        match cmd {
            Command::ListRuns => {
                send_resp(&resp_tx, Response::Runs(service.list_runs().await));
            }
            Command::ListExperiments => {
                send_resp(
                    &resp_tx,
                    Response::Experiments(service.list_experiments().await),
                );
            }
            Command::ListProjects => {
                send_resp(&resp_tx, Response::Projects(service.list_projects().await));
            }
            Command::ListMetrics { run_id } => {
                let result = service.list_metrics(&run_id).await;
                send_resp(&resp_tx, Response::Metrics { run_id, result });
            }
            Command::Query { query } => {
                let result = service.query(&query).await;
                send_resp(&resp_tx, Response::Series { query, result });
            }
            Command::QueryBatch { request } => {
                let result = service.query_batch(&request).await;
                send_resp(&resp_tx, Response::BatchSeries { request, result });
            }
            Command::Subscribe { query } => {
                let _ = service.subscribe(&query).await;
            }
            Command::Unsubscribe { subscription_id } => {
                let _ = service.unsubscribe(subscription_id).await;
            }
            Command::CheckFinalised { run_id } => {
                if let Ok(true) = service.is_finalised(&run_id).await {
                    send_resp(&resp_tx, Response::Finalised { run_id });
                }
            }
        }
        ctx.request_repaint();
    }
}
