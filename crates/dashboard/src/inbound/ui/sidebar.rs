use egui::{RichText, Ui};

use photon_core::types::id::RunId;

use super::theme;

pub struct SidebarState {
    pub search_query: String,
    pub selected_runs: Vec<RunId>,
}

impl Default for SidebarState {
    fn default() -> Self {
        Self {
            search_query: String::new(),
            selected_runs: Vec::new(),
        }
    }
}

pub enum SidebarAction {
    SelectRun(RunId),
    ToggleRun(RunId),
    ClearSelection,
}

pub fn show(
    ui: &mut Ui,
    state: &mut SidebarState,
    runs: &[RunId],
) -> Option<SidebarAction> {
    let mut action: Option<SidebarAction> = None;

    ui.heading(RichText::new("Runs").color(theme::TEXT_PRIMARY));
    ui.add_space(6.0);

    ui.horizontal(|ui| {
        ui.label("Search:");
        ui.text_edit_singleline(&mut state.search_query);
    });
    ui.add_space(4.0);

    ui.separator();
    ui.add_space(4.0);

    let filtered: Vec<&RunId> = runs
        .iter()
        .filter(|r| matches_search(r, &state.search_query))
        .collect();

    for run_id in &filtered {
        let is_selected = state.selected_runs.contains(run_id);

        let label = RichText::new(run_id.short()).color(if is_selected {
            theme::ACCENT
        } else {
            theme::TEXT_PRIMARY
        });

        let response = ui.selectable_label(is_selected, label);
        if response.clicked() {
            let modifier = ui.input(|i| i.modifiers.command);
            if modifier {
                action = Some(SidebarAction::ToggleRun(**run_id));
            } else if is_selected && state.selected_runs.len() == 1 {
                action = Some(SidebarAction::ClearSelection);
            } else {
                action = Some(SidebarAction::SelectRun(**run_id));
            }
        }
    }

    action
}

fn matches_search(run_id: &RunId, query: &str) -> bool {
    if query.is_empty() {
        return true;
    }
    let q = query.to_lowercase();
    run_id.short().to_lowercase().contains(&q)
}
