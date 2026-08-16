//! Settings → Archived (feature-inventory §1.5): archived chats across
//! devices, with Unarchive (Mutate setChatArchived false).

use gpui::{
    AnyElement, Context, Entity, SharedString, Subscription, Task, Window, div, prelude::*, px,
};

use zeron_proto::{Chat, Space};
use zeron_rpc::methods;

use crate::state::AppState;
use crate::theme::Theme;

/// Archived rows in sidebar (recency) order. Pure.
pub fn archived_chats(chats: &[Chat]) -> Vec<&Chat> {
    chats.iter().filter(|c| c.archived).collect()
}

struct ArchivedProjectGroup {
    key: String,
    name: String,
    chats: Vec<Chat>,
}

/// Groups archived threads by their owning project while keeping each
/// project's session order intact. A missing space remains visible under a
/// truthful fallback heading instead of disappearing from history.
fn archived_project_groups(chats: &[Chat], spaces: &[Space]) -> Vec<ArchivedProjectGroup> {
    let mut groups: Vec<ArchivedProjectGroup> = Vec::new();
    for chat in archived_chats(chats).into_iter().cloned() {
        let space = chat
            .space_id
            .as_deref()
            .and_then(|id| spaces.iter().find(|space| space.id == id));
        let (key, name) = match space {
            Some(space) => (space.id.clone(), space.display_name().to_string()),
            None if chat.space_id.is_some() => (
                chat.space_id.clone().unwrap_or_default(),
                "Unknown project".to_string(),
            ),
            None => ("__no-project__".to_string(), "No project".to_string()),
        };
        if let Some(group) = groups.iter_mut().find(|group| group.key == key) {
            group.chats.push(chat);
        } else {
            groups.push(ArchivedProjectGroup {
                key,
                name,
                chats: vec![chat],
            });
        }
    }
    groups.sort_by(|a, b| {
        (a.name == "No project", a.name.to_lowercase(), a.key.clone()).cmp(&(
            b.name == "No project",
            b.name.to_lowercase(),
            b.key.clone(),
        ))
    });
    groups
}

pub struct ArchivedPage {
    state: Entity<AppState>,
    error: Option<SharedString>,
    /// Chat with an in-flight unarchive (button shows working state).
    busy: Option<String>,
    /// Row index under the pointer — drives the original's `group-hover`
    /// Unarchive reveal (`opacity-0 group-hover:opacity-100`).
    hovered: Option<usize>,
    task: Option<Task<()>>,
    _observe: Subscription,
}

impl ArchivedPage {
    pub fn new(state: Entity<AppState>, cx: &mut Context<Self>) -> Self {
        let observe = cx.observe(&state, |_, _, cx| cx.notify());
        Self {
            state,
            error: None,
            busy: None,
            hovered: None,
            task: None,
            _observe: observe,
        }
    }

    fn unarchive(&mut self, chat_id: String, cx: &mut Context<Self>) {
        let Some(engine) = self.state.read(cx).engine().cloned() else {
            return;
        };
        self.busy = Some(chat_id.clone());
        self.error = None;
        let params = serde_json::json!({
            "op": "setChatArchived",
            "chatId": chat_id,
            "archived": false,
        });
        self.task = Some(cx.spawn(async move |this, cx| {
            let result = engine.client().call(methods::MUTATE, params).await;
            this.update(cx, |page, cx| {
                page.busy = None;
                if let Err(err) = result {
                    page.error = Some(format!("Unarchive failed: {err}").into());
                }
                cx.notify();
            })
            .ok();
        }));
        cx.notify();
    }
}

impl Render for ArchivedPage {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        use crate::settings::widgets;
        let theme = Theme::of(cx).clone();
        let groups: Vec<ArchivedProjectGroup> = {
            let state = self.state.read(cx);
            archived_project_groups(&state.chats, &state.spaces)
        };
        let busy = self.busy.clone();
        let count: usize = groups.iter().map(|group| group.chats.len()).sum();
        let mut row_index = 0usize;
        let mut items: Vec<AnyElement> = Vec::new();

        for (group_ix, group) in groups.into_iter().enumerate() {
            let group_key = group.key.clone();
            let group_name: SharedString = group.name.into();
            let group_count = group.chats.len();
            items.push(
                div()
                    .id(SharedString::from(format!("archived-project-{group_key}")))
                    .mt(px(if group_ix == 0 { 0.0 } else { 18.0 }))
                    .h(px(30.0))
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap(px(8.0))
                    .px(px(12.0))
                    .child(
                        crate::icons::icon(crate::icons::FOLDER)
                            .size(px(18.0))
                            .text_color(theme.text_muted.opacity(0.75)),
                    )
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .truncate()
                            .text_size(px(13.0))
                            .font_weight(gpui::FontWeight::MEDIUM)
                            .text_color(theme.text_muted)
                            .child(group_name),
                    )
                    .child(
                        div()
                            .flex_none()
                            .text_size(px(11.0))
                            .text_color(theme.text_muted.opacity(0.55))
                            .child(SharedString::from(format!("{group_count}"))),
                    )
                    .into_any_element(),
            );

            for chat in group.chats {
                let ix = row_index;
                row_index += 1;
                let title: SharedString = chat
                    .title
                    .clone()
                    .unwrap_or_else(|| "Untitled session".into())
                    .into();
                let is_busy = busy.as_deref() == Some(chat.id.as_str());
                let row_hovered = self.hovered == Some(ix);
                let chat_id = chat.id.clone();
                // Archived rows stay intentionally minimal: project grouping
                // provides the context, while each row shows only its title.
                items.push(
                    div()
                        .id(("archived-row", ix))
                        .flex()
                        .flex_row()
                        .items_center()
                        .gap(px(12.0))
                        .rounded(px(8.0))
                        .px(px(12.0))
                        .py(px(8.0))
                        .hover(|s| s.bg(crate::theme::ink(0.03)))
                        .on_hover(cx.listener(move |this, hovered: &bool, _, cx| {
                            if *hovered {
                                this.hovered = Some(ix);
                            } else if this.hovered == Some(ix) {
                                this.hovered = None;
                            }
                            cx.notify();
                        }))
                        .child(
                            div()
                                .flex_none()
                                .size(px(32.0))
                                .rounded(px(6.0))
                                .border_1()
                                .border_color(theme.border)
                                .flex()
                                .items_center()
                                .justify_center()
                                .child(
                                    crate::icons::icon(crate::icons::ARCHIVE_MINIMALISTIC)
                                        .size(px(16.0))
                                        .text_color(theme.text_muted.opacity(0.6)),
                                ),
                        )
                        .child(
                            div().flex_1().min_w_0().flex().flex_col().child(
                                div()
                                    .min_w_0()
                                    .truncate()
                                    .text_size(px(13.0))
                                    .font_weight(gpui::FontWeight::MEDIUM)
                                    .text_color(theme.text)
                                    .child(title),
                            ),
                        )
                        .child(
                            // Hidden until the row is hovered (zeron `opacity-0
                            // group-hover:opacity-100`); hover fill is the solid
                            // accent tone (`hover:bg-accent`).
                            div()
                                .id(("unarchive", ix))
                                .flex_none()
                                .flex()
                                .flex_row()
                                .items_center()
                                .gap(px(6.0))
                                .px(px(10.0))
                                .py(px(4.0))
                                .rounded(px(6.0))
                                .border_1()
                                .border_color(theme.border)
                                .text_size(px(12.0))
                                .text_color(theme.text_muted)
                                .opacity(if row_hovered || is_busy { 1.0 } else { 0.0 })
                                .when(is_busy, |el| el.opacity(0.4))
                                .cursor_pointer()
                                .hover(|s| s.bg(theme.surface_raised).text_color(theme.text))
                                .on_click(cx.listener(move |this, _, _, cx| {
                                    this.unarchive(chat_id.clone(), cx);
                                }))
                                .child(
                                    crate::icons::icon(crate::icons::ARCHIVE_UP_MINIMALISTIC)
                                        .size(px(14.0))
                                        .text_color(theme.text_muted),
                                )
                                .child(SharedString::from(if is_busy {
                                    "Unarchiving…"
                                } else {
                                    "Unarchive"
                                })),
                        )
                        .into_any_element(),
                );
            }
        }

        let body: AnyElement = if items.is_empty() {
            // Centered empty state (zeron settings.archived.tsx).
            div()
                .mt(px(96.0))
                .flex()
                .flex_col()
                .items_center()
                .text_center()
                .text_color(theme.text_muted.opacity(0.5))
                .child(
                    // `opacity-40` on top of the inherited muted/50 — an
                    // effectively ~20% glyph (zeron settings.archived.tsx).
                    crate::icons::icon(crate::icons::ARCHIVE_MINIMALISTIC)
                        .size(px(28.0))
                        .text_color(theme.text_muted.opacity(0.2)),
                )
                .child(
                    div()
                        .mt(px(12.0))
                        .text_size(px(14.0))
                        .child(SharedString::from("Nothing archived")),
                )
                .child(
                    div()
                        .mt(px(4.0))
                        .text_size(px(12.0))
                        .text_color(theme.text_muted.opacity(0.4))
                        .child(SharedString::from(
                            "Archived threads are grouped here by project.",
                        )),
                )
                .into_any_element()
        } else {
            div()
                .mt(px(24.0))
                .flex()
                .flex_col()
                .gap(px(2.0))
                .children(items)
                .into_any_element()
        };

        div()
            .id("archived-page")
            .size_full()
            .overflow_y_scroll()
            .child(
                widgets::page_column()
                    .child(widgets::page_header(
                        &theme,
                        "Archived sessions",
                        (count > 0).then_some(count),
                    ))
                    .child(widgets::page_subtitle(
                        &theme,
                        "Hidden from the sidebar, never deleted. Unarchiving puts a session back on its device.",
                    ))
                    .when_some(self.error.clone(), |el, message| {
                        el.child(
                            widgets::error_strip(&theme, message)
                                .id("archived-error")
                                .cursor_pointer()
                                .on_click(cx.listener(|this, _, _, cx| {
                                    this.error = None;
                                    cx.notify();
                                })),
                        )
                    })
                    .child(body),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn chat(id: &str, archived: bool) -> Chat {
        Chat {
            id: id.into(),
            device_id: "d".into(),
            title: None,
            archived,
            cwd: None,
            branch: None,
            checkout_id: None,
            config: None,
            last_message_preview: None,
            last_message_at: None,
            created_at: Utc::now(),
            harness_session_id: None,
            harness_session_cwd: None,
            space_id: None,
            last_seen_at: None,
        }
    }

    fn chat_in_space(id: &str, archived: bool, space_id: Option<&str>) -> Chat {
        let mut chat = chat(id, archived);
        chat.space_id = space_id.map(str::to_owned);
        chat
    }

    fn space(id: &str, name: &str) -> Space {
        Space {
            id: id.into(),
            device_id: "d".into(),
            path: format!("/tmp/{name}"),
            name: Some(name.into()),
            git_detected: false,
            git_checked_at: None,
            checkout_id: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn only_archived_rows_show() {
        let chats = vec![chat("a", false), chat("b", true), chat("c", true)];
        let rows = archived_chats(&chats);
        let ids: Vec<&str> = rows.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["b", "c"]);
    }

    #[test]
    fn archived_rows_group_by_project() {
        let chats = vec![
            chat_in_space("beta-thread", true, Some("s2")),
            chat_in_space("alpha-first", true, Some("s1")),
            chat_in_space("active", false, Some("s1")),
            chat_in_space("no-project", true, None),
            chat_in_space("alpha-second", true, Some("s1")),
        ];
        let spaces = vec![space("s1", "Alpha"), space("s2", "Beta")];

        let groups = archived_project_groups(&chats, &spaces);

        assert_eq!(
            groups
                .iter()
                .map(|group| group.name.as_str())
                .collect::<Vec<_>>(),
            ["Alpha", "Beta", "No project"]
        );
        assert_eq!(
            groups
                .iter()
                .map(|group| group.chats.len())
                .collect::<Vec<_>>(),
            [2, 1, 1]
        );
        assert_eq!(
            groups[0]
                .chats
                .iter()
                .map(|chat| chat.id.as_str())
                .collect::<Vec<_>>(),
            ["alpha-first", "alpha-second"]
        );
    }
}
