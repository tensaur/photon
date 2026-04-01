use egui::{Color32, FontFamily, RichText, Stroke, Vec2, vec2};

use crate::theme::DARK;

pub fn show(ui: &mut egui::Ui, is_live: bool, project_name: &str, context_name: &str, search_query: &mut String) {
    let total_width = ui.available_width();

    ui.horizontal(|ui| {
        ui.set_height(crate::theme::TOP_BAR_HEIGHT);

        ui.add_space(12.0);
        ui.label(RichText::new("★").size(14.0).color(DARK.text_primary));
        ui.label(RichText::new(egui_phosphor::regular::CARET_DOWN).font(crate::theme::icon_font_id(10.0)).color(Color32::from_rgb(0x66, 0x66, 0x66)));
        ui.add_space(8.0);

        ui.label(
            RichText::new(egui_phosphor::regular::FILE)
                .font(crate::theme::icon_font_id(13.0))
                .color(DARK.text_primary),
        );
        ui.label(RichText::new(project_name).size(13.0).color(DARK.text_primary));
        ui.label(RichText::new(egui_phosphor::regular::STAR).font(crate::theme::icon_font_id(12.0)).color(Color32::from_rgb(0x66, 0x66, 0x66)));
        ui.label(RichText::new("/").size(12.0).color(Color32::from_rgb(0x55, 0x55, 0x55)));
        ui.label(
            RichText::new(egui_phosphor::regular::CHECK_SQUARE)
                .font(crate::theme::icon_font_id(13.0))
                .color(DARK.text_primary),
        );
        ui.label(
            RichText::new(context_name)
                .size(13.0)
                .strong()
                .color(DARK.text_primary),
        );

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
                        let (rect, _) = ui.allocate_exact_size(Vec2::splat(8.0), egui::Sense::hover());
                        ui.painter().circle_filled(
                            rect.center(),
                            3.5,
                            DARK.status_done,
                        );
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
                            .font(crate::theme::icon_font_id(13.0))
                            .color(DARK.text_dim),
                    );
                    ui.add(
                        egui::TextEdit::singleline(search_query)
                            .frame(false)
                            .hint_text("Search or command")
                            .desired_width(f32::INFINITY)
                            .text_color(DARK.text_primary),
                    );
                });
            });

        let used = ui.cursor().left() - ui.min_rect().left();
        let right_spacer = (total_width - used - right_content_est).max(8.0);
        ui.add_space(right_spacer);

        {
            let (rect, _) = ui.allocate_exact_size(Vec2::splat(24.0), egui::Sense::hover());
            ui.painter()
                .circle_filled(rect.center(), 12.0, Color32::from_rgb(0x44, 0x44, 0x44));
        }
        ui.add_space(8.0);

        {
            let mut job = egui::text::LayoutJob::default();
            job.append(
                egui_phosphor::regular::GRID_FOUR,
                0.0,
                egui::TextFormat {
                    font_id: crate::theme::icon_font_id(12.0),
                    color: DARK.text_secondary,
                    ..Default::default()
                },
            );
            job.append(
                " Add panel",
                4.0,
                egui::TextFormat {
                    font_id: egui::FontId::new(12.0, FontFamily::Proportional),
                    color: DARK.text_secondary,
                    ..Default::default()
                },
            );
            if ui.add(egui::Button::new(job).frame(false)).clicked() {}
        }

        ui.add_space(8.0);

        egui::Frame::NONE
            .stroke(Stroke::new(1.0, Color32::from_rgb(0x33, 0x33, 0x33)))
            .inner_margin(egui::Margin::symmetric(10, 6))
            .show(ui, |ui| {
                ui.label(RichText::new("Share").size(12.0).color(DARK.text_primary));
            });

        ui.add_space(12.0);
    });

    let rect = ui.min_rect();
    ui.painter().hline(
        rect.x_range(),
        rect.bottom(),
        Stroke::new(1.0, DARK.border),
    );
}
