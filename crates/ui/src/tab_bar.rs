use egui::{RichText, Stroke, vec2};

use crate::theme::DARK;

pub fn show(ui: &mut egui::Ui, active_tab: &str) {
    let tab_bar_height = crate::theme::TAB_BAR_HEIGHT;

    let full_rect = ui.available_rect_before_wrap();
    ui.painter()
        .rect_filled(full_rect, egui::CornerRadius::ZERO, DARK.bg);

    ui.horizontal(|ui| {
        ui.set_height(tab_bar_height);
        ui.spacing_mut().item_spacing = vec2(0.0, 0.0);

        let tab_text_size = 12.0;
        let underline_h = 2.0;

        let tab_width = ui
            .painter()
            .layout_no_wrap(
                active_tab.to_string(),
                egui::FontId::proportional(tab_text_size),
                DARK.text_primary,
            )
            .size()
            .x
            + 16.0;
        let (tab_rect, _) =
            ui.allocate_exact_size(vec2(tab_width, tab_bar_height), egui::Sense::hover());

        ui.painter().text(
            egui::pos2(tab_rect.center().x, tab_rect.center().y),
            egui::Align2::CENTER_CENTER,
            active_tab,
            egui::FontId::proportional(tab_text_size),
            DARK.text_primary,
        );

        ui.painter().hline(
            tab_rect.x_range(),
            tab_rect.bottom() - underline_h / 2.0,
            Stroke::new(underline_h, DARK.text_primary),
        );

        ui.add_space(4.0);
        ui.add(
            egui::Button::new(RichText::new("+").size(14.0).color(DARK.text_dim))
                .frame(false)
                .min_size(vec2(20.0, tab_bar_height)),
        );
    });

    let rect = ui.min_rect();
    ui.painter()
        .hline(rect.x_range(), rect.bottom(), Stroke::new(1.0, DARK.border));
}
