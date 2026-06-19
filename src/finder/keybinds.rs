//! Keybinding cheatsheet data, parsed from the embedded `KEYBINDS.toml`.
//!
//! `docs/KEYBINDS.toml` is the documented single source of truth for keybindings.
//! It is embedded at compile time so the `:Keymaps` picker ships in the binary.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

use super::FinderItem;

/// Raw entry as stored in KEYBINDS.toml. Mode + category come from the table path.
#[derive(Debug, Clone, Deserialize)]
struct RawKeybind {
    key: String,
    action: String,
    #[serde(default)]
    desc: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    vim_default: bool,
}

/// A single documented keybinding, flattened with its mode and category.
/// Some fields (`action`, `vim_default`) are carried for future introspection
/// features (which-key, keymap doctor) and not read yet.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct KeybindEntry {
    pub mode: String,
    pub category: String,
    pub key: String,
    pub action: String,
    pub desc: String,
    pub status: String,
    pub vim_default: bool,
}

/// A mode's entries are stored one of two ways in KEYBINDS.toml:
/// - `[[mode.category]]` — grouped by category (normal, visual, insert, leader, finder, commands)
/// - `[[mode]]` — a flat array with no category (terminal, text_objects, registers)
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum ModeSection {
    /// `[[mode]]` — entries directly under the mode.
    Flat(Vec<RawKeybind>),
    /// `[[mode.category]]` — entries grouped by category.
    Categorized(BTreeMap<String, Vec<RawKeybind>>),
}

/// Parse KEYBINDS.toml text into a flat, deterministically-ordered list.
///
/// Handles both the categorized (`[[mode.category]]`) and flat (`[[mode]]`)
/// table shapes. A parse failure yields an empty list.
pub fn parse_keybinds(raw: &str) -> Vec<KeybindEntry> {
    let parsed: BTreeMap<String, ModeSection> = match toml::from_str(raw) {
        Ok(parsed) => parsed,
        Err(_) => return Vec::new(),
    };

    let mut entries = Vec::new();
    for (mode, section) in parsed {
        match section {
            ModeSection::Flat(binds) => {
                for b in binds {
                    entries.push(make_entry(&mode, "", b));
                }
            }
            ModeSection::Categorized(categories) => {
                for (category, binds) in categories {
                    for b in binds {
                        entries.push(make_entry(&mode, &category, b));
                    }
                }
            }
        }
    }
    entries
}

fn make_entry(mode: &str, category: &str, b: RawKeybind) -> KeybindEntry {
    KeybindEntry {
        mode: mode.to_string(),
        category: category.to_string(),
        key: b.key,
        action: b.action,
        desc: b.desc,
        status: b.status,
        vim_default: b.vim_default,
    }
}

/// Load and parse the embedded KEYBINDS.toml.
pub fn load_keybinds() -> Vec<KeybindEntry> {
    const RAW: &str = include_str!("../../docs/KEYBINDS.toml");
    parse_keybinds(RAW)
}

/// Short mode tag for the display row (e.g. "normal" -> "n").
/// Provided for you to use inside `display_row`.
fn mode_tag(mode: &str) -> &str {
    match mode {
        "normal" => "n",
        "visual" => "v",
        "insert" => "i",
        "leader" => "leader",
        "commands" => "cmd",
        "finder" => "finder",
        "terminal" => "term",
        "text_objects" => "text-obj",
        "registers" => "reg",
        other => other,
    }
}

/// Format one keybinding as a single row shown in the `:Keymaps` picker.
pub fn display_row(entry: &KeybindEntry) -> String {
    let key = if entry.mode == "leader" {
        format!("<leader>{}", entry.key)
    } else {
        entry.key.clone()
    };
    format!("{:<7} {:<18} {}", mode_tag(&entry.mode), key, entry.desc)
}

/// Build finder items for every *implemented* keybinding.
pub fn keymap_finder_items() -> Vec<FinderItem> {
    keymap_items_from(load_keybinds())
}

/// Filter to implemented entries and render each as an inert finder item
/// (empty path, no buffer/terminal/git metadata) so Enter has nothing to act on.
fn keymap_items_from(entries: Vec<KeybindEntry>) -> Vec<FinderItem> {
    entries
        .into_iter()
        .filter(|entry| entry.status == "implemented")
        .map(|entry| FinderItem::new(display_row(&entry), PathBuf::new()).with_icon("  "))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[[normal.movement]]
key = "h"
action = "move_left"
desc = "Move cursor left"
status = "implemented"
vim_default = true

[[leader.files]]
key = "ff"
action = ":FindFiles<CR>"
desc = "Find files"
status = "implemented"

[[normal.lsp]]
key = "gD"
action = "goto_declaration"
desc = "Go to declaration"
status = "planned"
vim_default = true
"#;

    #[test]
    fn parses_mode_and_category_from_table_path() {
        let entries = parse_keybinds(SAMPLE);
        assert_eq!(entries.len(), 3);
        let h = entries.iter().find(|e| e.key == "h").expect("h entry");
        assert_eq!(h.mode, "normal");
        assert_eq!(h.category, "movement");
        assert_eq!(h.desc, "Move cursor left");
        assert_eq!(h.status, "implemented");
        assert!(h.vim_default);
    }

    #[test]
    fn items_include_only_implemented() {
        let items = keymap_items_from(parse_keybinds(SAMPLE));
        // h and ff are implemented; gD is planned and excluded
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn leader_rows_get_leader_prefix() {
        let entries = parse_keybinds(SAMPLE);
        let ff = entries.iter().find(|e| e.key == "ff").unwrap();
        let row = display_row(ff);
        assert!(row.contains("<leader>ff"), "row was: {}", row);
    }

    #[test]
    fn embedded_file_parses_and_is_fully_described() {
        let entries = load_keybinds();
        assert!(entries.len() > 250, "expected a substantial keymap, got {}", entries.len());
        assert!(entries.iter().all(|e| !e.desc.is_empty()), "every entry needs a description");
        assert!(entries.iter().any(|e| e.status == "implemented"));
    }
}
