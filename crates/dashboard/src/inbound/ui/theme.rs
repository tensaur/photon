use egui::{Color32, Visuals};

/// Main background (dark navy).
pub const BG_PRIMARY: Color32 = Color32::from_rgb(15, 17, 26);

/// Sidebar / panel background.
pub const BG_SIDEBAR: Color32 = Color32::from_rgb(20, 22, 34);

/// Slightly lighter surface for cards / pane bodies.
pub const BG_SURFACE: Color32 = Color32::from_rgb(28, 31, 46);

/// Subtle border between panels.
pub const BORDER: Color32 = Color32::from_rgb(48, 52, 70);

/// Primary accent (soft blue-purple).
pub const ACCENT: Color32 = Color32::from_rgb(0x88, 0x88, 0xFF);

/// Status: running.
pub const STATUS_RUNNING: Color32 = Color32::from_rgb(0x44, 0xDD, 0x88);

/// Status: finished.
pub const STATUS_FINISHED: Color32 = Color32::from_rgb(0xEE, 0xCC, 0x44);

/// Status: failed.
pub const STATUS_FAILED: Color32 = Color32::from_rgb(0xEE, 0x55, 0x55);

pub const TEXT_PRIMARY: Color32 = Color32::from_rgb(220, 222, 230);
pub const TEXT_DIM: Color32 = Color32::from_rgb(140, 145, 160);

pub const CHART_COLORS: &[Color32] = &[
    Color32::from_rgb(0x88, 0x88, 0xFF), // accent blue
    Color32::from_rgb(0xFF, 0x88, 0x55), // warm orange
    Color32::from_rgb(0x44, 0xDD, 0x88), // green
    Color32::from_rgb(0xFF, 0x55, 0xAA), // pink
    Color32::from_rgb(0x55, 0xCC, 0xEE), // cyan
    Color32::from_rgb(0xEE, 0xCC, 0x44), // gold
    Color32::from_rgb(0xBB, 0x77, 0xFF), // purple
    Color32::from_rgb(0xFF, 0x77, 0x77), // salmon
];

pub fn apply(ctx: &egui::Context) {
    let mut visuals = Visuals::dark();

    visuals.panel_fill = BG_PRIMARY;
    visuals.window_fill = BG_SURFACE;
    visuals.extreme_bg_color = BG_SIDEBAR;
    visuals.faint_bg_color = BG_SURFACE;

    visuals.widgets.noninteractive.bg_fill = BG_SURFACE;
    visuals.widgets.noninteractive.fg_stroke.color = TEXT_PRIMARY;
    visuals.widgets.noninteractive.bg_stroke.color = BORDER;

    visuals.widgets.inactive.bg_fill = BG_SURFACE;
    visuals.widgets.inactive.fg_stroke.color = TEXT_DIM;
    visuals.widgets.inactive.bg_stroke.color = BORDER;

    visuals.widgets.hovered.bg_fill = Color32::from_rgb(40, 44, 62);
    visuals.widgets.hovered.fg_stroke.color = TEXT_PRIMARY;
    visuals.widgets.hovered.bg_stroke.color = ACCENT;

    visuals.widgets.active.bg_fill = Color32::from_rgb(50, 54, 72);
    visuals.widgets.active.fg_stroke.color = TEXT_PRIMARY;
    visuals.widgets.active.bg_stroke.color = ACCENT;

    visuals.selection.bg_fill = Color32::from_rgba_premultiplied(0x88, 0x88, 0xFF, 40);
    visuals.selection.stroke.color = ACCENT;

    ctx.set_visuals(visuals);
}
