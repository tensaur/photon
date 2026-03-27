use std::collections::HashMap;

use egui::{Color32, RichText, Ui};

use photon_core::domain::experiment::Experiment;
use photon_core::domain::run::{Run, RunStatus};
use photon_core::types::id::{ExperimentId, RunId};

use super::theme;

pub struct SidebarState {
    pub search_query: String,
    pub show_running: bool,
    pub show_finished: bool,
    pub show_failed: bool,
    pub selected_runs: Vec<RunId>,
    pub expanded_experiments: HashMap<Option<ExperimentId>, bool>,
}

impl Default for SidebarState {
    fn default() -> Self {
        Self {
            search_query: String::new(),
            show_running: true,
            show_finished: true,
            show_failed: true,
            selected_runs: Vec::new(),
            expanded_experiments: HashMap::new(),
        }
    }
}

pub enum SidebarAction {
    /// Replace selection with this single run.
    SelectRun(RunId),
    /// Toggle a run in/out of the selection (ctrl/cmd+click).
    ToggleRun(RunId),
    /// Clear selection entirely.
    ClearSelection,
}

pub fn show(
    ui: &mut Ui,
    state: &mut SidebarState,
    runs: &[Run],
    experiments: &[Experiment],
) -> Option<SidebarAction> {
    let mut action: Option<SidebarAction> = None;

    ui.heading(RichText::new("Runs").color(theme::TEXT_PRIMARY));
    ui.add_space(6.0);

    ui.horizontal(|ui| {
        ui.label("Search:");
        ui.text_edit_singleline(&mut state.search_query);
    });
    ui.add_space(4.0);

    ui.horizontal(|ui| {
        status_toggle(
            ui,
            "Running",
            theme::STATUS_RUNNING,
            &mut state.show_running,
        );
        status_toggle(
            ui,
            "Finished",
            theme::STATUS_FINISHED,
            &mut state.show_finished,
        );
        status_toggle(ui, "Failed", theme::STATUS_FAILED, &mut state.show_failed);
    });
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

    for exp_id in exp_ids {
        let exp_runs = &by_experiment[&exp_id];
        let expanded = state.expanded_experiments.entry(exp_id).or_insert(true);

        let header_text = match exp_id {
            Some(id) => experiments
                .iter()
                .find(|e| e.id == id).map_or_else(|| format!("Experiment {}", id.short()), |e| e.name.clone()),
            None => "Ungrouped".to_string(),
        };
        let header = egui::CollapsingHeader::new(RichText::new(header_text).color(theme::TEXT_DIM))
            .id_salt(exp_id)
            .default_open(*expanded)
            .show(ui, |ui| {
                for run in exp_runs {
                    let is_selected = state.selected_runs.contains(&run.id());
                    let status_color = status_color(run.status());

                    ui.horizontal(|ui| {
                        // Status dot.
                        let (rect, _) =
                            ui.allocate_exact_size(egui::vec2(8.0, 8.0), egui::Sense::hover());
                        ui.painter().circle_filled(rect.center(), 4.0, status_color);

                        let label = RichText::new(run.name()).color(if is_selected {
                            theme::ACCENT
                        } else {
                            theme::TEXT_PRIMARY
                        });

                        let response = ui.selectable_label(is_selected, label);
                        if response.clicked() {
                            let modifier = ui.input(|i| i.modifiers.command);
                            if modifier {
                                // Ctrl/Cmd+click: toggle run in/out of selection
                                action = Some(SidebarAction::ToggleRun(run.id()));
                            } else if is_selected && state.selected_runs.len() == 1 {
                                // Click on sole selected run: deselect
                                action = Some(SidebarAction::ClearSelection);
                            } else {
                                // Normal click: replace selection with this run
                                action = Some(SidebarAction::SelectRun(run.id()));
                            }
                        }
                    });
                }
            });
        *expanded = header.fully_open();
    }

    action
}

fn status_toggle(ui: &mut Ui, label: &str, color: Color32, value: &mut bool) {
    let text = RichText::new(label).color(if *value { color } else { theme::TEXT_DIM });
    if ui.selectable_label(*value, text).clicked() {
        *value = !*value;
    }
}

fn status_color(status: &RunStatus) -> Color32 {
    match status {
        RunStatus::Running => theme::STATUS_RUNNING,
        RunStatus::Finished => theme::STATUS_FINISHED,
        RunStatus::Failed { .. } => theme::STATUS_FAILED,
        RunStatus::Created => theme::TEXT_DIM,
    }
}

fn matches_status(run: &Run, state: &SidebarState) -> bool {
    match run.status() {
        RunStatus::Running | RunStatus::Created => state.show_running,
        RunStatus::Finished => state.show_finished,
        RunStatus::Failed { .. } => state.show_failed,
    }
}

fn matches_search(run: &Run, query: &str) -> bool {
    if query.is_empty() {
        return true;
    }
    let q = query.to_lowercase();
    run.name().to_lowercase().contains(&q) || run.tags().iter().any(|t| t.to_lowercase().contains(&q))
}
