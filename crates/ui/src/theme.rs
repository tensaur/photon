use std::collections::BTreeMap;
use std::sync::Arc;

use egui::{
    Color32, CornerRadius, FontData, FontDefinitions, FontFamily, FontId, Stroke, TextStyle,
    Visuals, epaint::Shadow,
};

const ICON_FAMILY_NAME: &str = "icons";

/// Returns a `FontId` for rendering Phosphor icons at the given size.
pub fn icon_font_id(size: f32) -> FontId {
    FontId::new(size, FontFamily::Name(ICON_FAMILY_NAME.into()))
}

pub const TOP_BAR_HEIGHT: f32 = 44.0;
pub const ICON_RAIL_WIDTH: f32 = 36.0;
pub const SIDEBAR_WIDTH: f32 = 210.0;
pub const TAB_BAR_HEIGHT: f32 = 28.0;
pub const PANEL_GAP: f32 = 3.0;

pub struct Theme {
    pub bg: Color32,
    pub surface: Color32,
    pub border: Color32,
    pub text_primary: Color32,
    pub text_secondary: Color32,
    pub text_dim: Color32,
    pub status_done: Color32,
    pub status_running: Color32,
    pub status_failed: Color32,
    pub live_border: Color32,
    pub chart_colors: [Color32; 8],
}

pub static DARK: Theme = Theme {
    bg: Color32::from_rgb(0x0D, 0x0E, 0x12),
    surface: Color32::from_rgb(0x18, 0x1A, 0x1F),
    border: Color32::from_rgb(0x1E, 0x20, 0x28),
    text_primary: Color32::from_rgb(0xE8, 0xE8, 0xEE),
    text_secondary: Color32::from_rgb(0x99, 0x99, 0x99),
    text_dim: Color32::from_rgb(0x55, 0x55, 0x55),
    status_done: Color32::from_rgb(0x44, 0xDD, 0x88),
    status_running: Color32::from_rgb(0xDD, 0xAA, 0x33),
    status_failed: Color32::from_rgb(0xEE, 0x55, 0x55),
    live_border: Color32::from_rgb(0x1E, 0x4D, 0x2E),
    chart_colors: [
        Color32::from_rgb(0x55, 0x88, 0xFF),
        Color32::from_rgb(0xFF, 0x99, 0x33),
        Color32::from_rgb(0x44, 0xDD, 0x88),
        Color32::from_rgb(0xFF, 0x55, 0xAA),
        Color32::from_rgb(0x55, 0xDD, 0xEE),
        Color32::from_rgb(0xDD, 0xAA, 0x33),
        Color32::from_rgb(0xAA, 0x66, 0xFF),
        Color32::from_rgb(0xFF, 0x66, 0x44),
    ],
};

pub fn apply(ctx: &egui::Context, theme: &Theme) {
    let mut fonts = FontDefinitions::empty();

    fonts.font_data.insert(
        "inter_medium".into(),
        Arc::new(FontData::from_static(include_bytes!(
            "../data/fonts/Inter-Medium.otf"
        ))),
    );

    fonts.font_data.insert(
        "jetbrains_mono".into(),
        Arc::new(FontData::from_static(include_bytes!(
            "../data/fonts/JetBrainsMono-Regular.ttf"
        ))),
    );

    fonts.font_data.insert(
        "phosphor".into(),
        egui_phosphor::Variant::Regular.font_data().into(),
    );

    fonts.families.insert(
        FontFamily::Proportional,
        vec!["inter_medium".into()],
    );

    fonts
        .families
        .insert(FontFamily::Monospace, vec!["jetbrains_mono".into()]);

    fonts.families.insert(
        FontFamily::Name(ICON_FAMILY_NAME.into()),
        vec!["phosphor".into(), "inter_medium".into()],
    );

    ctx.set_fonts(fonts);

    let text_styles: BTreeMap<TextStyle, FontId> = [
        (
            TextStyle::Body,
            FontId::new(12.0, FontFamily::Proportional),
        ),
        (
            TextStyle::Small,
            FontId::new(10.0, FontFamily::Proportional),
        ),
        (
            TextStyle::Button,
            FontId::new(12.0, FontFamily::Proportional),
        ),
        (
            TextStyle::Heading,
            FontId::new(13.0, FontFamily::Proportional),
        ),
        (
            TextStyle::Monospace,
            FontId::new(12.0, FontFamily::Monospace),
        ),
    ]
    .into();

    let widget_noninteractive = egui::style::WidgetVisuals {
        bg_fill: theme.surface,
        weak_bg_fill: theme.surface,
        bg_stroke: Stroke::new(1.0, theme.border),
        corner_radius: CornerRadius::ZERO,
        fg_stroke: Stroke::new(1.0, theme.text_secondary),
        expansion: 0.0,
    };

    let widget_inactive = egui::style::WidgetVisuals {
        bg_fill: theme.surface,
        weak_bg_fill: Color32::TRANSPARENT,
        bg_stroke: Stroke::new(1.0, theme.border),
        corner_radius: CornerRadius::ZERO,
        fg_stroke: Stroke::new(1.0, theme.text_primary),
        expansion: 0.0,
    };

    let widget_hovered = egui::style::WidgetVisuals {
        bg_fill: theme.border,
        weak_bg_fill: theme.border,
        bg_stroke: Stroke::new(1.0, theme.text_dim),
        corner_radius: CornerRadius::ZERO,
        fg_stroke: Stroke::new(1.5, theme.text_primary),
        expansion: 1.0,
    };

    let widget_active = egui::style::WidgetVisuals {
        bg_fill: theme.border,
        weak_bg_fill: theme.border,
        bg_stroke: Stroke::new(1.0, theme.text_secondary),
        corner_radius: CornerRadius::ZERO,
        fg_stroke: Stroke::new(2.0, theme.text_primary),
        expansion: 1.0,
    };

    let widget_open = egui::style::WidgetVisuals {
        bg_fill: theme.surface,
        weak_bg_fill: theme.surface,
        bg_stroke: Stroke::new(1.0, theme.border),
        corner_radius: CornerRadius::ZERO,
        fg_stroke: Stroke::new(1.0, theme.text_primary),
        expansion: 0.0,
    };

    let mut visuals = Visuals {
        dark_mode: true,
        override_text_color: Some(theme.text_primary),
        widgets: egui::style::Widgets {
            noninteractive: widget_noninteractive,
            inactive: widget_inactive,
            hovered: widget_hovered,
            active: widget_active,
            open: widget_open,
        },
        faint_bg_color: theme.surface,
        extreme_bg_color: theme.bg,
        code_bg_color: theme.surface,
        warn_fg_color: theme.status_running,
        error_fg_color: theme.status_failed,
        window_fill: theme.surface,
        window_stroke: Stroke::new(1.0, theme.border),
        window_corner_radius: CornerRadius::ZERO,
        window_shadow: Shadow::NONE,
        panel_fill: theme.bg,
        popup_shadow: Shadow::NONE,
        menu_corner_radius: CornerRadius::ZERO,
        ..Visuals::dark()
    };
    visuals.selection.bg_fill = Color32::from_rgb(0x55, 0x88, 0xFF).gamma_multiply(0.3);
    visuals.selection.stroke = Stroke::new(1.0, Color32::from_rgb(0x55, 0x88, 0xFF));

    let spacing = egui::style::Spacing {
        item_spacing: egui::vec2(6.0, 4.0),
        button_padding: egui::vec2(6.0, 2.0),
        interact_size: egui::vec2(40.0, 20.0),
        ..Default::default()
    };

    ctx.set_style(egui::Style {
        text_styles,
        visuals,
        spacing,
        ..Default::default()
    });
}
