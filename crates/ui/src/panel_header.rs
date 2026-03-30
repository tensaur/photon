use egui::{RichText, Sense, Stroke, Vec2, vec2};

use crate::theme::DARK;

const RIGHT_ICONS: &[&str] = &[
    egui_phosphor::regular::DOTS_THREE,
    egui_phosphor::regular::ARROWS_OUT,
    egui_phosphor::regular::GRID_FOUR,
    egui_phosphor::regular::CHART_LINE_UP,
    egui_phosphor::regular::PLUS,
];

pub fn show(ui: &mut egui::Ui, title: &str) {
    let v_pad = 6.0;
    let h_pad = 10.0;
    let icon_size = 13.0;

    // Draw surface background
    let full_rect = ui.available_rect_before_wrap();
    ui.painter().rect_filled(full_rect, egui::CornerRadius::ZERO, DARK.surface);

    ui.horizontal(|ui| {
        ui.spacing_mut().item_spacing = vec2(4.0, 0.0);
        ui.set_min_height(icon_size + v_pad * 2.0);

        ui.add_space(h_pad);

        ui.label(
            RichText::new(egui_phosphor::regular::DOTS_SIX)
                .size(10.0)
                .color(DARK.text_secondary),
        );
        ui.add_space(4.0);
        ui.label(RichText::new(title).size(12.0).color(DARK.text_primary));

        let available = ui.available_width();
        // Push right by consuming the available space minus the icons width.
        let icon_total = RIGHT_ICONS.len() as f32 * (icon_size + 6.0) + h_pad;
        if available > icon_total {
            ui.add_space(available - icon_total);
        }

        for &icon in RIGHT_ICONS {
            let (rect, response) = ui.allocate_exact_size(
                Vec2::splat(icon_size + 6.0),
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
                egui::FontId::proportional(icon_size),
                color,
            );
            // No action yet — placeholder buttons
        }

        ui.add_space(h_pad);
    });

    // 1px bottom border
    let rect = ui.min_rect();
    ui.painter().hline(
        rect.x_range(),
        rect.bottom(),
        Stroke::new(1.0, DARK.border),
    );
}
