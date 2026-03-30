use std::collections::HashMap;

use egui_tiles::Tiles;

use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};
use photon_core::types::query::{MetricQuery, MetricSeries, SeriesData};

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
    sidebar_visible: bool,
    icon_rail_state: photon_ui::icon_rail::IconRailState,
    crosshair_x: Option<f64>,
}

impl DashboardApp {
    pub fn new(
        cc: &eframe::CreationContext<'_>,
        commands: CommandSender,
        responses: ResponseReceiver,
    ) -> Self {
        photon_ui::theme::apply(&cc.egui_ctx, &photon_ui::theme::DARK);

        // Request initial run and experiment lists.
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
            sidebar_visible: true,
            icon_rail_state: photon_ui::icon_rail::IconRailState::default(),
            crosshair_x: None,
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
                    // Rebuild viewport now that new metrics have arrived.
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
            Response::BatchSeries { .. } | Response::SubscriptionEnded { .. } => {}
            Response::LivePoints {
                run_id,
                metric,
                points,
            } => {
                if let Some(series) = self.cache.series.get_mut(&(run_id, metric))
                    && let SeriesData::Raw {
                        points: ref mut existing,
                    } = series.data
                {
                    existing.extend(points);
                }
            }
            Response::RunsChanged => {
                // Re-fetch the run and experiment lists
                self.cache.runs = RequestState::Pending;
                self.cache.experiments = RequestState::Pending;
                channel::send_cmd(&self.commands, Command::ListRuns);
                channel::send_cmd(&self.commands, Command::ListExperiments);
            }
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_sidebar_action(&mut self, action: SidebarAction) {
        match action {
            SidebarAction::SelectRun(run_id) => {
                // Clear all other visible runs — show only this one.
                self.unsubscribe_all();
                self.sidebar.visible_runs.clear();
                self.sidebar.run_colors.clear();

                self.sidebar.selected_runs = vec![run_id];
                self.sidebar.visible_runs.push(run_id);
                self.sidebar.assign_color(run_id);
                self.ensure_metrics_loaded(run_id);
                self.subscribe_if_active(run_id);
                self.rebuild_viewport();
            }
            SidebarAction::ToggleVisibility(run_id) => {
                if let Some(pos) = self.sidebar.visible_runs.iter().position(|&id| id == run_id) {
                    // Hide this run.
                    self.sidebar.visible_runs.remove(pos);
                    self.sidebar.release_color(&run_id);
                    self.sidebar.selected_runs.retain(|&id| id != run_id);
                    channel::send_cmd(&self.commands, Command::Unsubscribe { run_id });
                } else {
                    // Show this run (add to existing visible set).
                    self.sidebar.visible_runs.push(run_id);
                    self.sidebar.assign_color(run_id);
                    self.ensure_metrics_loaded(run_id);
                    self.subscribe_if_active(run_id);
                }
                // Select the toggled run for focus.
                self.sidebar.selected_runs = vec![run_id];
                self.rebuild_viewport();
            }
            SidebarAction::MakeVisible(run_id) => {
                // Drag-select: add to visible set without clearing others.
                if !self.sidebar.visible_runs.contains(&run_id) {
                    self.sidebar.visible_runs.push(run_id);
                    self.sidebar.assign_color(run_id);
                    self.ensure_metrics_loaded(run_id);
                    self.subscribe_if_active(run_id);
                    self.rebuild_viewport();
                }
            }
            SidebarAction::ClearSelection => {
                self.unsubscribe_all();
                self.sidebar.selected_runs.clear();
                self.sidebar.visible_runs.clear();
                self.sidebar.run_colors.clear();
                self.tile_tree = None;
            }
        }
    }

    fn subscribe_if_active(&self, run_id: RunId) {
        if let RequestState::Loaded(runs) = &self.cache.runs
            && let Some(run) = runs.iter().find(|r| r.id() == run_id)
            && run.is_active()
        {
            channel::send_cmd(&self.commands, Command::Subscribe { run_id });
        }
    }

    fn unsubscribe_all(&self) {
        for &run_id in &self.sidebar.visible_runs {
            channel::send_cmd(&self.commands, Command::Unsubscribe { run_id });
        }
    }

    fn ensure_metrics_loaded(&mut self, run_id: RunId) {
        if let std::collections::hash_map::Entry::Vacant(e) = self.cache.metrics.entry(run_id) {
            e.insert(RequestState::Pending);
            channel::send_cmd(&self.commands, Command::ListMetrics { run_id });
        }
    }

    fn rebuild_viewport(&mut self) {
        let selected = &self.sidebar.visible_runs;
        if selected.is_empty() {
            self.tile_tree = None;
            return;
        }

        // Collect metrics for visible runs that have loaded (don't wait for all).
        let mut per_run_metrics: Vec<(RunId, Vec<Metric>)> = Vec::new();
        for &run_id in selected {
            if let Some(RequestState::Loaded(metrics)) = self.cache.metrics.get(&run_id) {
                per_run_metrics.push((run_id, metrics.clone()));
            }
        }

        if per_run_metrics.is_empty() {
            return; // nothing loaded yet
        }

        // Request data for any runs we haven't queried yet
        for (run_id, metrics) in &per_run_metrics {
            for m in metrics {
                if self.cache.get_series(run_id, m).is_none() {
                    channel::send_cmd(
                        &self.commands,
                        Command::Query {
                            query: MetricQuery {
                                run_id: *run_id,
                                key: m.clone(),
                                step_range: Step::ZERO..Step::MAX,
                                target_points: 1000,
                            },
                        },
                    );
                }
            }
        }

        let mut tiles = Tiles::default();

        let visible = &self.sidebar.visible_runs;

        if visible.len() == 1 {
            // Single run: one line chart per metric
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
            // Multiple runs: comparison panes for shared metrics
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

        // Check if any run is live
        let is_live = matches!(&self.cache.runs, RequestState::Loaded(runs) if runs.iter().any(|r| r.is_active()));

        // 1. Top bar (full width, 44px)
        egui::TopBottomPanel::top("top_bar")
            .exact_height(photon_ui::theme::TOP_BAR_HEIGHT)
            .frame(egui::Frame::NONE.fill(theme.bg))
            .show(ctx, |ui| {
                photon_ui::top_bar::show(ui, is_live);
            });

        // 2. Icon rail (left, 36px)
        egui::SidePanel::left("icon_rail")
            .exact_width(photon_ui::theme::ICON_RAIL_WIDTH)
            .resizable(false)
            .frame(egui::Frame::NONE.fill(theme.bg).stroke(egui::Stroke::new(1.0, theme.border)))
            .show(ctx, |ui| {
                if photon_ui::icon_rail::show(ui, &mut self.icon_rail_state) {
                    self.sidebar_visible = !self.sidebar_visible;
                }
            });

        // 3. Sidebar (collapsible, 210px)
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
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        let runs_slice: &[Run] = match &self.cache.runs {
                            RequestState::Loaded(runs) => runs.as_slice(),
                            _ => &[],
                        };
                        let experiments_slice: &[Experiment] = match &self.cache.experiments {
                            RequestState::Loaded(experiments) => experiments.as_slice(),
                            _ => &[],
                        };

                        let action = sidebar::show(ui, &mut self.sidebar, runs_slice, experiments_slice);

                        match &self.cache.runs {
                            RequestState::Pending => { ui.spinner(); }
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
                    }).inner
                })
                .inner
        } else {
            Vec::new()
        };

        for action in sidebar_actions {
            self.handle_sidebar_action(action);
        }

        // 4. Central panel (tab bar + viewport)
        egui::CentralPanel::default()
            .frame(egui::Frame::NONE.fill(theme.bg))
            .show(ctx, |ui| {
                // Tab bar
                photon_ui::tab_bar::show(ui, "Overview");

                // Viewport
                if let Some(tree) = &mut self.tile_tree {
                    let mut crosshair_x = self.crosshair_x.take();
                    let mut behavior = ViewportBehavior {
                        cache: &self.cache,
                        sidebar_state: &self.sidebar,
                        crosshair_x: &mut crosshair_x,
                    };
                    tree.ui(&mut behavior, ui);
                    self.crosshair_x = crosshair_x;
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
