use egui::{Color32, Sense, Vec2, vec2};

use crate::theme::DARK;

const ICONS: &[&str] = &[
    egui_phosphor::regular::GRID_FOUR,
    egui_phosphor::regular::CHART_LINE_UP,
    egui_phosphor::regular::CLOCK,
    egui_phosphor::regular::CROSSHAIR,
    egui_phosphor::regular::GRAPH,
];

#[derive(Default)]
pub struct IconRailState {
    pub active_index: usize,
}

/// Returns true if the already-active icon was clicked (VS Code-style sidebar toggle).
pub fn show(ui: &mut egui::Ui, state: &mut IconRailState) -> bool {
    let mut toggle = false;

    ui.vertical_centered(|ui| {
        ui.add_space(12.0);
        ui.spacing_mut().item_spacing = vec2(0.0, 4.0);

        for (i, &icon) in ICONS.iter().enumerate() {
            let is_active = state.active_index == i;
            let color = if is_active {
                DARK.text_primary
            } else {
                Color32::from_rgb(0x77, 0x77, 0x77)
            };

            let (rect, response) = ui.allocate_exact_size(Vec2::splat(28.0), Sense::click());

            if response.hovered() {
                ui.painter().rect_filled(
                    rect,
                    egui::CornerRadius::ZERO,
                    Color32::from_rgba_unmultiplied(0xFF, 0xFF, 0xFF, 8),
                );
            }

            ui.painter().text(
                rect.center(),
                egui::Align2::CENTER_CENTER,
                icon,
                crate::theme::icon_font_id(18.0),
                color,
            );

            if response.clicked() {
                if is_active {
                    toggle = true;
                } else {
                    state.active_index = i;
                }
            }
        }
    });

    toggle
}
