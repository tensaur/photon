use egui::{RichText, Sense, Stroke, Vec2, vec2};

use crate::theme::DARK;

const RIGHT_ICONS: &[&str] = &[
    egui_phosphor::regular::PLUS,
    egui_phosphor::regular::CHART_LINE_UP,
    egui_phosphor::regular::GRID_FOUR,
    egui_phosphor::regular::ARROWS_OUT,
    egui_phosphor::regular::DOTS_THREE,
];

/// Returns true if the drag handle was dragged (signals tile rearrangement).
pub fn show(ui: &mut egui::Ui, title: &str) -> bool {
    let icon_size = 13.0;
    let header_height = 26.0;

    // Use Frame for background so it paints BEFORE content.
    let frame_resp = egui::Frame::NONE
        .fill(DARK.surface)
        .inner_margin(egui::Margin::symmetric(8, 0))
        .show(ui, |ui| {
            ui.set_height(header_height);
            ui.horizontal_centered(|ui| {
                ui.spacing_mut().item_spacing = vec2(4.0, 0.0);

                // Drag handle
                let (drag_rect, drag_response) = ui.allocate_exact_size(
                    Vec2::new(14.0, header_height),
                    Sense::click_and_drag(),
                );
                ui.painter().text(
                    drag_rect.center(),
                    egui::Align2::CENTER_CENTER,
                    egui_phosphor::regular::DOTS_SIX,
                    crate::theme::icon_font_id(10.0),
                    DARK.text_secondary,
                );
                if drag_response.hovered() {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::Grab);
                }
                if drag_response.dragged() {
                    ui.ctx().set_cursor_icon(egui::CursorIcon::Grabbing);
                }
                let dragged = drag_response.dragged();

                // Title
                ui.add_space(4.0);
                ui.add(
                    egui::Label::new(
                        RichText::new(title).size(12.0).color(DARK.text_primary),
                    )
                    .truncate(),
                );

                // Right-aligned icon buttons
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    for &icon in RIGHT_ICONS.iter() {
                        let (rect, response) = ui.allocate_exact_size(
                            Vec2::splat(icon_size + 4.0),
                            Sense::click(),
                        );
                        let color = if response.hovered() {
                            DARK.text_primary
                        } else {
                            DARK.text_secondary
                        };
                        ui.painter().text(
                            rect.center(),
                            egui::Align2::CENTER_CENTER,
                            icon,
                            crate::theme::icon_font_id(icon_size),
                            color,
                        );
                    }
                });

                dragged
            })
            .inner
        });

    // Bottom border
    let rect = frame_resp.response.rect;
    ui.painter().hline(
        rect.x_range(),
        rect.bottom(),
        Stroke::new(1.0, DARK.border),
    );

    frame_resp.inner
}
