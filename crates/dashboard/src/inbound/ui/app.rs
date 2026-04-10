use std::collections::{HashMap, HashSet};

use egui_tiles::Tiles;

use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::{Run, RunStatus};
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};
use photon_core::types::id::SubscriptionId;
use photon_core::types::query::{MetricQuery, MetricSeries, SeriesData};
use photon_core::types::stream::DeltaData;

use crate::inbound::channel::{self, Command, CommandSender, Response, ResponseReceiver};

use super::panes::{ComparisonState, LineChartState, Pane};
use super::sidebar::{self, SidebarAction, SidebarState};
use super::viewport::ViewportBehavior;

#[derive(Default)]
pub enum RequestState<T> {
    #[default]
    Idle,
    Pending,
    Loaded(T),
    Failed(String),
}

pub struct DataCache {
    pub runs: RequestState<Vec<Run>>,
    pub experiments: RequestState<Vec<Experiment>>,
    pub projects: RequestState<Vec<Project>>,
    pub metrics: HashMap<RunId, RequestState<Vec<Metric>>>,
    pub series: HashMap<(RunId, Metric), MetricSeries>,
    /// Subscriptions we've already sent commands for. Keyed by `(run_id, metric)`
    /// to avoid double-subscribing on repeated `rebuild_viewport` calls; populated
    /// when the command is issued and resolved to a `SubscriptionId` when the
    /// first `Snapshot` arrives.
    pub subscribed_keys: HashSet<(RunId, Metric)>,
    /// Reverse map: `SubscriptionId → (run_id, metric)`, populated from `Snapshot`.
    /// Used to route `Delta` / `Unsubscribed` messages, which
    /// only carry the id.
    pub subscriptions: HashMap<SubscriptionId, (RunId, Metric)>,
    /// Runs the server has marked as finalised — fully persisted and indexed.
    /// Populated from `StreamFrame::RunFinalised`; drives the sidebar status
    /// dot for `RunStatus::Finished` runs.
    pub finalised: HashSet<RunId>,
}

impl Default for DataCache {
    fn default() -> Self {
        Self {
            runs: RequestState::Idle,
            experiments: RequestState::Idle,
            projects: RequestState::Idle,
            metrics: HashMap::new(),
            series: HashMap::new(),
            subscribed_keys: HashSet::new(),
            subscriptions: HashMap::new(),
            finalised: HashSet::new(),
        }
    }
}

impl DataCache {
    pub fn get_series(&self, run_id: &RunId, metric: &Metric) -> Option<&MetricSeries> {
        self.series.get(&(*run_id, metric.clone()))
    }

    pub fn run_name(&self, run_id: &RunId) -> Option<String> {
        if let RequestState::Loaded(runs) = &self.runs {
            runs.iter()
                .find(|r| r.id() == *run_id)
                .map(|r| r.name().to_string())
        } else {
            None
        }
    }
}

pub struct DashboardApp {
    commands: CommandSender,
    responses: ResponseReceiver,
    cache: DataCache,
    sidebar: SidebarState,
    tile_tree: Option<egui_tiles::Tree<Pane>>,
    sidebar_visible: bool,
    icon_rail_state: photon_ui::icon_rail::IconRailState,
    crosshair_x: Option<f64>,
    expanded_tile: Option<egui_tiles::TileId>,
}

impl DashboardApp {
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        commands: CommandSender,
        responses: ResponseReceiver,
    ) -> Self {
        photon_ui::theme::apply(&cc.egui_ctx, &photon_ui::theme::DARK);

        channel::send_cmd(&commands, Command::ListRuns);
        channel::send_cmd(&commands, Command::ListExperiments);
        channel::send_cmd(&commands, Command::ListProjects);

        Self {
            commands,
            responses,
            cache: DataCache {
                runs: RequestState::Pending,
                experiments: RequestState::Pending,
                projects: RequestState::Pending,
                ..DataCache::default()
            },
            sidebar: SidebarState::default(),
            tile_tree: None,
            sidebar_visible: true,
            icon_rail_state: photon_ui::icon_rail::IconRailState::default(),
            crosshair_x: None,
            expanded_tile: None,
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
                Ok(runs) => {
                    for run in &runs {
                        if matches!(run.status(), RunStatus::Finished)
                            && !self.cache.finalised.contains(&run.id())
                        {
                            channel::send_cmd(
                                &self.commands,
                                Command::CheckFinalised { run_id: run.id() },
                            );
                        }
                    }
                    self.cache.runs = RequestState::Loaded(runs);
                }
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
            Response::Snapshot {
                subscription_id,
                series,
            } => {
                let key = (series.run_id, series.key.clone());
                self.cache
                    .subscriptions
                    .insert(subscription_id, key.clone());
                self.cache.series.insert(key, series);
            }
            Response::Delta {
                subscription_id,
                data,
            } => {
                let Some(key) = self.cache.subscriptions.get(&subscription_id).cloned() else {
                    return;
                };
                let Some(series) = self.cache.series.get_mut(&key) else {
                    return;
                };
                match (&mut series.data, data) {
                    (SeriesData::Raw { points }, DeltaData::RawPoints(new_points)) => {
                        points.extend(new_points);
                    }
                    (SeriesData::Bucketed { buckets }, DeltaData::Buckets(new_buckets)) => {
                        buckets.extend(new_buckets);
                    }
                    // Variant mismatch means the subscription was resnapshotted between
                    // the delta being sent and arriving here. Drop it.
                    _ => {}
                }
            }
            Response::Unsubscribed { subscription_id } => {
                if let Some(key) = self.cache.subscriptions.remove(&subscription_id) {
                    self.cache.subscribed_keys.remove(&key);
                }
            }
            Response::Finalised { run_id } => {
                self.cache.finalised.insert(run_id);
            }
            Response::RunsChanged => {
                self.cache.runs = RequestState::Pending;
                self.cache.experiments = RequestState::Pending;
                channel::send_cmd(&self.commands, Command::ListRuns);
                channel::send_cmd(&self.commands, Command::ListExperiments);
            }
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_sidebar_action(&mut self, action: SidebarAction) {
        // Subscriptions are driven by `rebuild_viewport`: any (run, metric) pair
        // that gets rendered is subscribed once and held for the process lifetime.
        // Visibility toggles only affect what's rendered, not what's subscribed.
        match action {
            SidebarAction::SelectRun(run_id) => {
                self.sidebar.visible_runs.clear();
                self.sidebar.run_colors.clear();

                self.sidebar.selected_runs = vec![run_id];
                self.sidebar.visible_runs.push(run_id);
                self.sidebar.assign_color(run_id);
                self.ensure_metrics_loaded(run_id);
                self.rebuild_viewport();
            }
            SidebarAction::ToggleVisibility(run_id) => {
                if let Some(pos) = self
                    .sidebar
                    .visible_runs
                    .iter()
                    .position(|&id| id == run_id)
                {
                    self.sidebar.visible_runs.remove(pos);
                    self.sidebar.release_color(&run_id);
                    self.sidebar.selected_runs.retain(|&id| id != run_id);
                } else {
                    self.sidebar.visible_runs.push(run_id);
                    self.sidebar.assign_color(run_id);
                    self.ensure_metrics_loaded(run_id);
                }
                self.sidebar.selected_runs = vec![run_id];
                self.rebuild_viewport();
            }
            SidebarAction::MakeVisible(run_id) => {
                if !self.sidebar.visible_runs.contains(&run_id) {
                    self.sidebar.visible_runs.push(run_id);
                    self.sidebar.assign_color(run_id);
                    self.ensure_metrics_loaded(run_id);
                    self.rebuild_viewport();
                }
            }
        }
    }

    fn ensure_metrics_loaded(&mut self, run_id: RunId) {
        if let std::collections::hash_map::Entry::Vacant(e) = self.cache.metrics.entry(run_id) {
            e.insert(RequestState::Pending);
            channel::send_cmd(&self.commands, Command::ListMetrics { run_id });
        }
    }

    fn rebuild_viewport(&mut self) {
        self.expanded_tile = None;

        let selected = &self.sidebar.visible_runs;
        if selected.is_empty() {
            self.tile_tree = None;
            return;
        }

        let mut per_run_metrics: Vec<(RunId, Vec<Metric>)> = Vec::new();
        for &run_id in selected {
            if let Some(RequestState::Loaded(metrics)) = self.cache.metrics.get(&run_id) {
                per_run_metrics.push((run_id, metrics.clone()));
            }
        }

        if per_run_metrics.is_empty() {
            return; // nothing loaded yet
        }

        for (run_id, metrics) in &per_run_metrics {
            for m in metrics {
                let key = (*run_id, m.clone());
                if self.cache.subscribed_keys.contains(&key) {
                    continue;
                }
                self.cache.subscribed_keys.insert(key);
                channel::send_cmd(
                    &self.commands,
                    Command::Subscribe {
                        query: MetricQuery {
                            run_id: *run_id,
                            key: m.clone(),
                            step_range: Step::ZERO..Step::MAX,
                            target_points: 100,
                        },
                    },
                );
            }
        }

        let mut tiles = Tiles::default();

        let visible = &self.sidebar.visible_runs;

        if visible.len() == 1 {
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
                return; // no metrics yet — keep existing viewport
            }
            let root = tiles.insert_grid_tile(tab_ids);
            self.tile_tree = Some(egui_tiles::Tree::new("viewport", root, tiles));
        } else {
            let shared_metrics = find_shared_metrics(&per_run_metrics);
            let run_ids: Vec<RunId> = per_run_metrics.iter().map(|(id, _)| *id).collect();

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
                return; // no shared metrics — keep existing viewport
            }
            let root = tiles.insert_grid_tile(tab_ids);
            self.tile_tree = Some(egui_tiles::Tree::new("viewport", root, tiles));
        }
    }
}

impl eframe::App for DashboardApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_responses();

        let theme = &photon_ui::theme::DARK;

        let is_live = matches!(&self.cache.runs, RequestState::Loaded(runs) if runs.iter().any(Run::is_active));

        let project_name = match &self.cache.projects {
            RequestState::Loaded(projects) => {
                projects.first().map_or("Photon", |p| p.name.as_str())
            }
            _ => "Photon",
        };
        let experiment_name = match &self.cache.experiments {
            RequestState::Loaded(experiments) => {
                if let Some(&sel) = self.sidebar.selected_runs.first() {
                    if let RequestState::Loaded(runs) = &self.cache.runs {
                        let exp_id = runs
                            .iter()
                            .find(|r| r.id() == sel)
                            .and_then(Run::experiment_id);
                        exp_id
                            .and_then(|eid| experiments.iter().find(|e| e.id == eid))
                            .map_or("Dashboard", |e| e.name.as_str())
                    } else {
                        "Dashboard"
                    }
                } else {
                    experiments.first().map_or("Dashboard", |e| e.name.as_str())
                }
            }
            _ => "Dashboard",
        };

        egui::TopBottomPanel::top("top_bar")
            .exact_height(photon_ui::theme::TOP_BAR_HEIGHT)
            .frame(egui::Frame::NONE.fill(theme.bg))
            .show(ctx, |ui| {
                photon_ui::top_bar::show(
                    ui,
                    is_live,
                    project_name,
                    experiment_name,
                    &mut self.sidebar.search_query,
                );
            });

        egui::SidePanel::left("icon_rail")
            .exact_width(photon_ui::theme::ICON_RAIL_WIDTH)
            .resizable(false)
            .frame(
                egui::Frame::NONE
                    .fill(theme.bg)
                    .stroke(egui::Stroke::new(1.0, theme.border)),
            )
            .show(ctx, |ui| {
                if photon_ui::icon_rail::show(ui, &mut self.icon_rail_state) {
                    self.sidebar_visible = !self.sidebar_visible;
                }
            });

        let sidebar_actions = if self.sidebar_visible {
            egui::SidePanel::left("sidebar")
                .exact_width(photon_ui::theme::SIDEBAR_WIDTH)
                .resizable(false)
                .frame(
                    egui::Frame::NONE
                        .fill(theme.bg)
                        .stroke(egui::Stroke::new(1.0, theme.border))
                        .inner_margin(egui::Margin::symmetric(12, 10)),
                )
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

                            let action = sidebar::show(
                                ui,
                                &mut self.sidebar,
                                runs_slice,
                                experiments_slice,
                                &self.cache.finalised,
                            );

                            match &self.cache.runs {
                                RequestState::Pending => {
                                    ui.spinner();
                                }
                                RequestState::Failed(msg) => {
                                    ui.colored_label(theme.status_failed, format!("Error: {msg}"));
                                    if ui.button("Retry").clicked() {
                                        channel::send_cmd(&self.commands, Command::ListRuns);
                                        self.cache.runs = RequestState::Pending;
                                    }
                                }
                                _ => {}
                            }

                            action
                        })
                        .inner
                })
                .inner
        } else {
            Vec::new()
        };

        for action in sidebar_actions {
            self.handle_sidebar_action(action);
        }

        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(theme.bg))
            .show(ctx, |ui| {
                photon_ui::tab_bar::show(ui, "Overview");

                if let Some(tree) = &mut self.tile_tree {
                    let mut crosshair_x = self.crosshair_x.take();
                    let mut expanded_tile = self.expanded_tile;

                    if let Some(exp_id) = self.expanded_tile {
                        // Render just the expanded pane directly (no tree layout).
                        if let Some(egui_tiles::Tile::Pane(pane)) = tree.tiles.get_mut(exp_id) {
                            egui::Frame::NONE.fill(theme.surface).show(ui, |ui| {
                                let header = photon_ui::panel_header::show(ui, pane.title());
                                if header.expand_clicked {
                                    expanded_tile = None;
                                }
                                match pane {
                                    super::panes::Pane::LineChart(state) => {
                                        super::panes::line_chart::show(
                                            ui,
                                            state,
                                            &self.cache,
                                            &self.sidebar,
                                            &mut crosshair_x,
                                        );
                                    }
                                    super::panes::Pane::Comparison(state) => {
                                        super::panes::comparison::show(
                                            ui,
                                            state,
                                            &self.cache,
                                            &self.sidebar,
                                            &mut crosshair_x,
                                        );
                                    }
                                }
                            });
                        } else {
                            expanded_tile = None;
                        }
                    } else {
                        let mut behavior = ViewportBehavior {
                            cache: &self.cache,
                            sidebar_state: &self.sidebar,
                            crosshair_x: &mut crosshair_x,
                            expanded_tile: &mut expanded_tile,
                        };
                        tree.ui(&mut behavior, ui);
                    }

                    self.crosshair_x = crosshair_x;
                    self.expanded_tile = expanded_tile;
                } else {
                    ui.centered_and_justified(|ui| {
                        ui.label(
                            egui::RichText::new("Select a run to view metrics")
                                .color(theme.text_dim)
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
