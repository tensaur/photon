use std::collections::{HashMap, HashSet};

use egui::{Color32, RichText, Sense, Stroke, StrokeKind, Ui, vec2};
use photon_ui::egui_phosphor::regular::{EYE, EYE_SLASH, MAGNIFYING_GLASS};

use photon_core::domain::experiment::Experiment;
use photon_core::domain::run::{Run, RunStatus};
use photon_core::types::id::{ExperimentId, RunId};

use photon_ui::theme;

const COLOR_COUNT: usize = 8;

pub struct SidebarState {
    pub search_query: String,
    pub show_done: bool,
    pub show_running: bool,
    pub show_failed: bool,
    pub selected_runs: Vec<RunId>,
    pub visible_runs: Vec<RunId>,
    pub expanded_experiments: HashMap<Option<ExperimentId>, bool>,
    pub run_colors: HashMap<RunId, usize>,
    next_color: usize,
    /// Y coordinate where a drag-select started (None = no drag in progress).
    drag_start_y: Option<f32>,
}

impl Default for SidebarState {
    fn default() -> Self {
        Self {
            search_query: String::new(),
            show_done: true,
            show_running: true,
            show_failed: true,
            selected_runs: Vec::new(),
            visible_runs: Vec::new(),
            expanded_experiments: HashMap::new(),
            run_colors: HashMap::new(),
            next_color: 0,
            drag_start_y: None,
        }
    }
}

impl SidebarState {
    pub fn assign_color(&mut self, run_id: RunId) -> usize {
        let idx = self.next_color % COLOR_COUNT;
        self.run_colors.insert(run_id, idx);
        self.next_color += 1;
        idx
    }

    pub fn release_color(&mut self, run_id: &RunId) {
        self.run_colors.remove(run_id);
    }

    pub fn get_color(&self, run_id: &RunId) -> Option<Color32> {
        self.run_colors
            .get(run_id)
            .map(|&idx| theme::DARK.chart_colors[idx % COLOR_COUNT])
    }
}

pub enum SidebarAction {
    SelectRun(RunId),
    ToggleVisibility(RunId),
    MakeVisible(RunId),
}

pub fn show(
    ui: &mut Ui,
    state: &mut SidebarState,
    runs: &[Run],
    experiments: &[Experiment],
    finalised: &HashSet<RunId>,
) -> Vec<SidebarAction> {
    let mut actions: Vec<SidebarAction> = Vec::new();

    let search_frame = egui::Frame::new()
        .fill(theme::DARK.surface)
        .stroke(Stroke::new(1.0, theme::DARK.border))
        .corner_radius(egui::CornerRadius::ZERO)
        .inner_margin(egui::Margin::symmetric(6, 4));

    search_frame.show(ui, |ui| {
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 4.0;
            ui.label(
                RichText::new(MAGNIFYING_GLASS)
                    .font(photon_ui::theme::icon_font_id(12.0))
                    .color(theme::DARK.text_dim),
            );
            let te = egui::TextEdit::singleline(&mut state.search_query)
                .frame(false)
                .hint_text("Search runs")
                .desired_width(f32::INFINITY);
            ui.add(te);
        });
    });
    ui.add_space(6.0);

    status_filter_bar(ui, state);
    ui.add_space(6.0);

    ui.separator();
    ui.add_space(4.0);

    let filtered: Vec<&Run> = runs
        .iter()
        .filter(|r| matches_status(r, state))
        .filter(|r| matches_search(r, &state.search_query))
        .collect();

    let mut by_experiment: HashMap<Option<ExperimentId>, Vec<&Run>> = HashMap::new();
    for run in &filtered {
        by_experiment
            .entry(run.experiment_id())
            .or_default()
            .push(run);
    }

    let mut exp_ids: Vec<Option<ExperimentId>> = by_experiment.keys().copied().collect();
    exp_ids.sort();

    let mut run_rows: Vec<(RunId, egui::Rect)> = Vec::new();

    for exp_id in exp_ids {
        let exp_runs = &by_experiment[&exp_id];
        let run_count = exp_runs.len();
        let expanded = *state.expanded_experiments.entry(exp_id).or_insert(true);

        let header_text = match exp_id {
            Some(id) => experiments.iter().find(|e| e.id == id).map_or_else(
                || format!("EXPERIMENT {}", id.short()),
                |e| e.name.to_uppercase(),
            ),
            None => "UNGROUPED".to_string(),
        };

        let chevron = if expanded {
            photon_ui::egui_phosphor::regular::CARET_DOWN
        } else {
            photon_ui::egui_phosphor::regular::CARET_RIGHT
        };

        let header_resp = ui
            .horizontal(|ui| {
                ui.label(
                    RichText::new(chevron)
                        .font(photon_ui::theme::icon_font_id(10.0))
                        .color(theme::DARK.text_secondary),
                );
                let resp = ui.add(
                    egui::Label::new(
                        RichText::new(&header_text)
                            .color(theme::DARK.text_secondary)
                            .size(10.0),
                    )
                    .selectable(false)
                    .sense(Sense::click()),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(
                        RichText::new(format!("{run_count}"))
                            .color(theme::DARK.text_dim)
                            .size(10.0),
                    );
                });
                resp
            })
            .inner;

        if header_resp.clicked() {
            let entry = state.expanded_experiments.entry(exp_id).or_insert(true);
            *entry = !*entry;
        }

        ui.add_space(10.0);

        if expanded {
            for run in exp_runs {
                let is_selected = state.selected_runs.contains(&run.id());
                let is_visible = state.visible_runs.contains(&run.id());

                let row_color = if is_selected {
                    theme::DARK.surface
                } else {
                    Color32::TRANSPARENT
                };

                let row_frame = egui::Frame::new()
                    .fill(row_color)
                    .inner_margin(egui::Margin::symmetric(4, 2));

                let row_frame_resp = row_frame.show(ui, |ui| {
                    let mut row_action: Option<SidebarAction> = None;
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 6.0;

                        let (swatch_rect, _) =
                            ui.allocate_exact_size(vec2(11.0, 11.0), Sense::hover());
                        let painter = ui.painter();
                        let cr = egui::CornerRadius::same(2);
                        if is_visible {
                            if let Some(color) = state.get_color(&run.id()) {
                                painter.rect_filled(swatch_rect, cr, color);
                            } else {
                                painter.rect_filled(swatch_rect, cr, theme::DARK.text_dim);
                            }
                        } else {
                            painter.rect_stroke(
                                swatch_rect,
                                cr,
                                Stroke::new(1.0, Color32::from_rgb(0x44, 0x44, 0x44)),
                                StrokeKind::Inside,
                            );
                        }

                        let name_color = if is_selected {
                            theme::DARK.text_primary
                        } else if is_visible {
                            theme::DARK.text_secondary
                        } else {
                            theme::DARK.text_dim
                        };

                        let name_resp = ui.add(
                            egui::Label::new(
                                RichText::new(run.name()).color(name_color).size(12.0),
                            )
                            .selectable(false)
                            .sense(Sense::click()),
                        );
                        if name_resp.hovered() {
                            ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                        }
                        if name_resp.clicked() {
                            if is_selected && is_visible {
                                row_action = Some(SidebarAction::ToggleVisibility(run.id()));
                            } else {
                                row_action = Some(SidebarAction::SelectRun(run.id()));
                            }
                        }

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let eye_color = if is_visible {
                                theme::DARK.text_secondary
                            } else {
                                theme::DARK.text_dim.gamma_multiply(0.3)
                            };
                            let eye_char = if is_visible { EYE } else { EYE_SLASH };
                            let eye_resp = ui.add(
                                egui::Label::new(
                                    RichText::new(eye_char)
                                        .font(photon_ui::theme::icon_font_id(12.0))
                                        .color(eye_color),
                                )
                                .selectable(false)
                                .sense(Sense::click()),
                            );
                            if eye_resp.hovered() {
                                ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
                            }
                            if eye_resp.clicked() {
                                row_action = Some(SidebarAction::ToggleVisibility(run.id()));
                            }

                            let (dot_rect, _) =
                                ui.allocate_exact_size(vec2(7.0, 7.0), Sense::hover());
                            ui.painter().circle_filled(
                                dot_rect.center(),
                                3.5,
                                status_color(run.status(), finalised.contains(&run.id())),
                            );
                        });
                    });
                    row_action
                });
                let row_rect = row_frame_resp.response.rect;
                let row_action = row_frame_resp.inner;

                if let Some(a) = row_action {
                    actions.push(a);
                }

                run_rows.push((run.id(), row_rect));
            }

            ui.add_space(8.0);
        }
    }

    let (primary_down, just_pressed, pointer_pos) = ui.input(|i| {
        (
            i.pointer.primary_down(),
            i.pointer.primary_pressed(),
            i.pointer.interact_pos(),
        )
    });

    if primary_down {
        if let Some(pos) = pointer_pos {
            if just_pressed {
                let on_row = run_rows.iter().any(|(_, rect)| rect.contains(pos));
                if on_row {
                    state.drag_start_y = Some(pos.y);
                }
            }

            if let Some(start_y) = state.drag_start_y {
                let y_min = start_y.min(pos.y);
                let y_max = start_y.max(pos.y);

                for &(run_id, row_rect) in &run_rows {
                    let row_center_y = row_rect.center().y;
                    if row_center_y >= y_min
                        && row_center_y <= y_max
                        && !state.visible_runs.contains(&run_id)
                    {
                        actions.push(SidebarAction::MakeVisible(run_id));
                    }
                }
            }
        }
    } else {
        state.drag_start_y = None;
    }

    actions
}

fn status_filter_bar(ui: &mut Ui, state: &mut SidebarState) {
    let bar_height = 22.0;
    let available_width = ui.available_width();
    let cell_width = available_width / 3.0;

    let (bar_rect, bar_resp) =
        ui.allocate_exact_size(vec2(available_width, bar_height), Sense::click());

    let painter = ui.painter();

    painter.rect_stroke(
        bar_rect,
        egui::CornerRadius::ZERO,
        Stroke::new(1.0, theme::DARK.border),
        StrokeKind::Inside,
    );

    let cells = [
        ("● Done", theme::DARK.status_done, state.show_done),
        ("● Running", theme::DARK.status_running, state.show_running),
        ("● Failed", theme::DARK.status_failed, state.show_failed),
    ];

    for (i, (label, color, active)) in cells.iter().enumerate() {
        let x = bar_rect.min.x + i as f32 * cell_width;
        let cell_rect =
            egui::Rect::from_min_size(egui::pos2(x, bar_rect.min.y), vec2(cell_width, bar_height));

        let bg = if *active {
            theme::DARK.surface
        } else {
            theme::DARK.bg
        };
        painter.rect_filled(cell_rect, egui::CornerRadius::ZERO, bg);

        if i > 0 {
            painter.line_segment(
                [egui::pos2(x, bar_rect.min.y), egui::pos2(x, bar_rect.max.y)],
                Stroke::new(1.0, theme::DARK.border),
            );
        }

        let text_color = *color;
        let galley = painter.layout_no_wrap(
            label.to_string(),
            egui::FontId::new(10.0, egui::FontFamily::Proportional),
            text_color,
        );
        let text_pos = egui::pos2(
            cell_rect.center().x - galley.size().x / 2.0,
            cell_rect.center().y - galley.size().y / 2.0,
        );
        painter.galley(text_pos, galley, text_color);
    }

    if bar_resp.clicked()
        && let Some(pos) = bar_resp.interact_pointer_pos()
    {
        let rel_x = pos.x - bar_rect.min.x;
        let cell_idx = (rel_x / cell_width) as usize;
        match cell_idx {
            0 => state.show_done = !state.show_done,
            1 => state.show_running = !state.show_running,
            2 => state.show_failed = !state.show_failed,
            _ => {}
        }
    }
}

fn status_color(status: &RunStatus, finalised: bool) -> Color32 {
    match status {
        RunStatus::Running | RunStatus::Created => theme::DARK.status_running,
        // A Finished run is still being processed by the persist pipeline
        // until the Finalised event arrives. Show that as a distinct state.
        RunStatus::Finished if !finalised => theme::DARK.status_processing,
        RunStatus::Finished => theme::DARK.status_done,
        RunStatus::Failed { .. } => theme::DARK.status_failed,
    }
}

fn matches_status(run: &Run, state: &SidebarState) -> bool {
    match run.status() {
        RunStatus::Running | RunStatus::Created => state.show_running,
        RunStatus::Finished => state.show_done,
        RunStatus::Failed { .. } => state.show_failed,
    }
}

fn matches_search(run: &Run, query: &str) -> bool {
    if query.is_empty() {
        return true;
    }
    let q = query.to_lowercase();
    run.name().to_lowercase().contains(&q)
        || run.tags().iter().any(|t| t.to_lowercase().contains(&q))
}
