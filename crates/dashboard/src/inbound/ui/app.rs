use std::collections::HashMap;

use egui_tiles::Tiles;

use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{MetricQuery, MetricSeries};

use crate::inbound::channel::{self, Command, CommandSender, Response, ResponseReceiver};

use super::panes::{ComparisonState, LineChartState, Pane};
use super::sidebar::{self, SidebarAction, SidebarState};
use super::theme;
use super::viewport::ViewportBehavior;

pub enum RequestState<T> {
    Idle,
    Pending,
    Loaded(T),
    Failed(String),
}

impl<T> Default for RequestState<T> {
    fn default() -> Self {
        Self::Idle
    }
}

pub struct DataCache {
    pub runs: RequestState<Vec<Run>>,
    pub experiments: RequestState<Vec<Experiment>>,
    pub projects: RequestState<Vec<Project>>,
    pub metrics: HashMap<RunId, RequestState<Vec<Metric>>>,
    pub series: HashMap<(RunId, Metric), MetricSeries>,
}

impl Default for DataCache {
    fn default() -> Self {
        Self {
            runs: RequestState::Idle,
            experiments: RequestState::Idle,
            projects: RequestState::Idle,
            metrics: HashMap::new(),
            series: HashMap::new(),
        }
    }
}

impl DataCache {
    pub fn get_series(&self, run_id: &RunId, metric: &Metric) -> Option<&MetricSeries> {
        self.series.get(&(*run_id, metric.clone()))
    }
}

pub struct DashboardApp {
    commands: CommandSender,
    responses: ResponseReceiver,
    cache: DataCache,
    sidebar: SidebarState,
    tile_tree: Option<egui_tiles::Tree<Pane>>,
}

impl DashboardApp {
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        commands: CommandSender,
        responses: ResponseReceiver,
    ) -> Self {
        theme::apply(&cc.egui_ctx);

        channel::send_cmd(&commands, Command::ListRuns);
        channel::send_cmd(&commands, Command::ListExperiments);

        Self {
            commands,
            responses,
            cache: DataCache {
                runs: RequestState::Pending,
                experiments: RequestState::Pending,
                ..DataCache::default()
            },
            sidebar: SidebarState::default(),
            tile_tree: None,
        }
    }

    fn drain_responses(&mut self) {
        while let Ok(resp) = self.responses.try_recv() {
            self.handle_response(resp);
        }
    }

    fn handle_response(&mut self, resp: Response) {
        match resp {
            Response::Runs(result) => match result {
                Ok(runs) => self.cache.runs = RequestState::Loaded(runs),
                Err(e) => self.cache.runs = RequestState::Failed(e.to_string()),
            },
            Response::Experiments(result) => match result {
                Ok(experiments) => self.cache.experiments = RequestState::Loaded(experiments),
                Err(e) => self.cache.experiments = RequestState::Failed(e.to_string()),
            },
            Response::Projects(result) => match result {
                Ok(projects) => self.cache.projects = RequestState::Loaded(projects),
                Err(e) => self.cache.projects = RequestState::Failed(e.to_string()),
            },
            Response::Metrics { run_id, result } => match result {
                Ok(metrics) => {
                    self.cache
                        .metrics
                        .insert(run_id, RequestState::Loaded(metrics));
                    self.rebuild_viewport();
                }
                Err(e) => {
                    self.cache
                        .metrics
                        .insert(run_id, RequestState::Failed(e.to_string()));
                }
            },
            Response::Series { query, result } => {
                if let Ok(series) = result {
                    let key = (query.run_id, query.key);
                    self.cache.series.insert(key, series);
                }
            }
            Response::BatchSeries { .. } => {}
        }
    }

    fn handle_sidebar_action(&mut self, action: SidebarAction) {
        match action {
            SidebarAction::SelectRun(run_id) => {
                self.unsubscribe_all();
                self.sidebar.selected_runs = vec![run_id];
                self.ensure_metrics_loaded(run_id);
                self.rebuild_viewport();
            }
            SidebarAction::ToggleRun(run_id) => {
                if let Some(pos) = self
                    .sidebar
                    .selected_runs
                    .iter()
                    .position(|&id| id == run_id)
                {
                    self.sidebar.selected_runs.remove(pos);
                    channel::send_cmd(&self.commands, Command::Unsubscribe { run_id });
                } else {
                    self.sidebar.selected_runs.push(run_id);
                    self.ensure_metrics_loaded(run_id);
                }
                self.rebuild_viewport();
            }
            SidebarAction::ClearSelection => {
                self.unsubscribe_all();
                self.sidebar.selected_runs.clear();
                self.tile_tree = None;
            }
        }
    }

    fn unsubscribe_all(&self) {
        for &run_id in &self.sidebar.selected_runs {
            channel::send_cmd(&self.commands, Command::Unsubscribe { run_id });
        }
    }

    fn ensure_metrics_loaded(&mut self, run_id: RunId) {
        if !self.cache.metrics.contains_key(&run_id) {
            self.cache.metrics.insert(run_id, RequestState::Pending);
            channel::send_cmd(&self.commands, Command::ListMetrics { run_id });
        }
    }

    fn rebuild_viewport(&mut self) {
        let selected = &self.sidebar.selected_runs;
        if selected.is_empty() {
            self.tile_tree = None;
            return;
        }

        let mut all_metrics_ready = true;
        let mut per_run_metrics: Vec<(RunId, Vec<Metric>)> = Vec::new();
        for &run_id in selected {
            match self.cache.metrics.get(&run_id) {
                Some(RequestState::Loaded(metrics)) => {
                    per_run_metrics.push((run_id, metrics.clone()));
                }
                _ => {
                    all_metrics_ready = false;
                }
            }
        }

        if !all_metrics_ready || per_run_metrics.is_empty() {
            return;
        }

        for (run_id, metrics) in &per_run_metrics {
            for m in metrics {
                if self.cache.get_series(run_id, m).is_none() {
                    channel::send_cmd(
                        &self.commands,
                        Command::Query {
                            query: MetricQuery {
                                run_id: *run_id,
                                key: m.clone(),
                                step_range: 0..u64::MAX,
                                target_points: 1000,
                            },
                        },
                    );
                }
            }
        }

        let mut tiles = Tiles::default();

        if selected.len() == 1 {
            let (run_id, metrics) = &per_run_metrics[0];
            let tab_ids: Vec<_> = metrics
                .iter()
                .map(|m| {
                    tiles.insert_pane(Pane::LineChart(LineChartState {
                        run_id: *run_id,
                        metric: m.clone(),
                    }))
                })
                .collect();
            if tab_ids.is_empty() {
                self.tile_tree = None;
                return;
            }
            let root = tiles.insert_grid_tile(tab_ids);
            self.tile_tree = Some(egui_tiles::Tree::new("viewport", root, tiles));
        } else {
            let shared_metrics = find_shared_metrics(&per_run_metrics);
            let run_ids: Vec<RunId> = selected.clone();

            let tab_ids: Vec<_> = shared_metrics
                .iter()
                .map(|m| {
                    tiles.insert_pane(Pane::Comparison(ComparisonState {
                        run_ids: run_ids.clone(),
                        metric: m.clone(),
                    }))
                })
                .collect();
            if tab_ids.is_empty() {
                self.tile_tree = None;
                return;
            }
            let root = tiles.insert_grid_tile(tab_ids);
            self.tile_tree = Some(egui_tiles::Tree::new("viewport", root, tiles));
        }
    }
}

impl eframe::App for DashboardApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_responses();

        let sidebar_action = egui::SidePanel::left("sidebar")
            .default_width(240.0)
            .resizable(true)
            .show(ctx, |ui| {
                egui::ScrollArea::vertical()
                    .show(ui, |ui| {
                        let runs_slice: &[Run] = match &self.cache.runs {
                            RequestState::Loaded(runs) => runs.as_slice(),
                            _ => &[],
                        };
                        let experiments_slice: &[Experiment] = match &self.cache.experiments {
                            RequestState::Loaded(experiments) => experiments.as_slice(),
                            _ => &[],
                        };
                        let action =
                            sidebar::show(ui, &mut self.sidebar, runs_slice, experiments_slice);

                        match &self.cache.runs {
                            RequestState::Pending => {
                                ui.spinner();
                            }
                            RequestState::Failed(msg) => {
                                ui.colored_label(theme::STATUS_FAILED, format!("Error: {msg}"));
                                if ui.button("Retry").clicked() {
                                    let _ = channel::send_cmd(&self.commands, Command::ListRuns);
                                    self.cache.runs = RequestState::Pending;
                                }
                            }
                            _ => {}
                        }

                        action
                    })
                    .inner
            })
            .inner;

        if let Some(action) = sidebar_action {
            self.handle_sidebar_action(action);
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(tree) = &mut self.tile_tree {
                let mut behavior = ViewportBehavior { cache: &self.cache };
                tree.ui(&mut behavior, ui);
            } else {
                ui.centered_and_justified(|ui| {
                    ui.label(
                        egui::RichText::new("Select a run to view metrics")
                            .color(theme::TEXT_DIM)
                            .size(18.0),
                    );
                });
            }
        });
    }
}

fn find_shared_metrics(per_run: &[(RunId, Vec<Metric>)]) -> Vec<Metric> {
    if per_run.is_empty() {
        return Vec::new();
    }
    let first = &per_run[0].1;
    first
        .iter()
        .filter(|m| per_run[1..].iter().all(|(_, metrics)| metrics.contains(m)))
        .cloned()
        .collect()
}
