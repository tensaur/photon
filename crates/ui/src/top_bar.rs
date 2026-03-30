use egui::{Color32, RichText, Stroke, Vec2, vec2};

use crate::theme::DARK;

pub fn show(ui: &mut egui::Ui, is_live: bool) {
    let total_width = ui.available_width();

    ui.horizontal(|ui| {
        ui.set_height(crate::theme::TOP_BAR_HEIGHT);

        ui.add_space(12.0);
        ui.label(RichText::new("★").size(14.0).color(DARK.text_primary));
        ui.label(RichText::new("▾").size(12.0).color(Color32::from_rgb(0x66, 0x66, 0x66)));
        ui.add_space(8.0);

        ui.label(
            RichText::new(egui_phosphor::regular::FILE)
                .size(13.0)
                .color(DARK.text_primary),
        );
        ui.label(RichText::new("my-project").size(13.0).color(DARK.text_primary));
        ui.label(RichText::new("☆").size(12.0).color(Color32::from_rgb(0x66, 0x66, 0x66)));
        ui.label(RichText::new("/").size(12.0).color(Color32::from_rgb(0x55, 0x55, 0x55)));
        ui.label(
            RichText::new(egui_phosphor::regular::CHECK_SQUARE)
                .size(13.0)
                .color(DARK.text_primary),
        );
        ui.label(
            RichText::new("training-exp")
                .size(13.0)
                .strong()
                .color(DARK.text_primary),
        );

        // Calculate how much space remains after left and right content,
        // leaving room to center the command bar.
        let cmd_bar_width = 280.0;
        let right_content_est = 200.0; // avatar + buttons estimate
        let left_content_est = ui.cursor().left() - ui.min_rect().left();
        let center_x = total_width / 2.0;
        let spacer = (center_x - left_content_est - cmd_bar_width / 2.0).max(8.0);
        ui.add_space(spacer);

        if is_live {
            let live_bg = Color32::from_rgba_unmultiplied(0x44, 0xDD, 0x88, 12);
            egui::Frame::NONE
                .fill(live_bg)
                .stroke(Stroke::new(1.0, DARK.live_border))
                .inner_margin(egui::Margin::symmetric(16, 6))
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing = vec2(4.0, 0.0);
                        // Green dot
                        let dot_pos = ui.cursor().min;
                        let (rect, _) = ui.allocate_exact_size(Vec2::splat(8.0), egui::Sense::hover());
                        ui.painter().circle_filled(
                            rect.center(),
                            3.5,
                            DARK.status_done,
                        );
                        let _ = dot_pos;
                        ui.label(
                            RichText::new("LIVE")
                                .size(12.0)
                                .strong()
                                .color(DARK.status_done),
                        );
                    });
                });
            ui.add_space(8.0);
        }

        egui::Frame::NONE
            .fill(DARK.surface)
            .stroke(Stroke::new(1.0, DARK.border))
            .inner_margin(egui::Margin::symmetric(10, 6))
            .show(ui, |ui| {
                ui.set_width(cmd_bar_width - 20.0); // subtract inner margin
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing = vec2(4.0, 0.0);
                    ui.label(
                        RichText::new(egui_phosphor::regular::MAGNIFYING_GLASS)
                            .size(13.0)
                            .color(DARK.text_dim),
                    );
                    ui.label(
                        RichText::new("Search or command")
                            .size(12.0)
                            .color(DARK.text_dim),
                    );
                });
            });

        let used = ui.cursor().left() - ui.min_rect().left();
        let right_spacer = (total_width - used - right_content_est).max(8.0);
        ui.add_space(right_spacer);

        // Avatar circle
        {
            let (rect, _) = ui.allocate_exact_size(Vec2::splat(24.0), egui::Sense::hover());
            ui.painter()
                .circle_filled(rect.center(), 12.0, Color32::from_rgb(0x44, 0x44, 0x44));
        }
        ui.add_space(8.0);

        // "Add panel" button
        if ui
            .add(
                egui::Button::new(
                    RichText::new(format!(
                        "{} Add panel",
                        egui_phosphor::regular::GRID_FOUR
                    ))
                    .size(12.0)
                    .color(DARK.text_secondary),
                )
                .frame(false),
            )
            .clicked()
        {}

        ui.add_space(8.0);

        // "Share" button
        egui::Frame::NONE
            .stroke(Stroke::new(1.0, Color32::from_rgb(0x33, 0x33, 0x33)))
            .inner_margin(egui::Margin::symmetric(10, 6))
            .show(ui, |ui| {
                ui.label(RichText::new("Share").size(12.0).color(DARK.text_primary));
            });

        ui.add_space(12.0);
    });

    // 1px bottom border
    let rect = ui.min_rect();
    ui.painter().hline(
        rect.x_range(),
        rect.bottom(),
        Stroke::new(1.0, DARK.border),
    );
}
