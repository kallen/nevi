use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use globset::{GlobBuilder, GlobMatcher};
use lsp_types::{
    DidChangeWatchedFilesParams, DidChangeWatchedFilesRegistrationOptions, FileChangeType,
    FileEvent, GlobPattern, OneOf, RegistrationParams, RelativePattern, UnregistrationParams, Url,
    WatchKind,
};
use notify::event::{ModifyKind, RenameMode};
#[cfg(not(test))]
use notify::RecommendedWatcher;
#[cfg(test)]
use notify::{Config, PollWatcher};
use notify::{Event, EventKind, RecursiveMode, Watcher};
use serde_json::json;

use super::client::SharedStdin;
use super::types::LspNotification;

pub(crate) const WATCHED_FILES_METHOD: &str = "workspace/didChangeWatchedFiles";

#[derive(Debug)]
pub(crate) enum WatcherRequestError {
    InvalidParams(String),
    #[allow(dead_code)] // Used by later registration routing when watcher setup can fail.
    Setup(String),
}

impl std::fmt::Display for WatcherRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(message) | Self::Setup(message) => formatter.write_str(message),
        }
    }
}

#[derive(Debug)]
pub(crate) struct WatchedFileRegistration {
    pub(crate) id: String,
    pub(crate) options: DidChangeWatchedFilesRegistrationOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchInput {
    Relative,
    Absolute,
}

#[derive(Clone)]
struct CompiledWatcher {
    base: PathBuf,
    root: PathBuf,
    matcher: GlobMatcher,
    input: MatchInput,
    kind: WatchKind,
}

#[derive(Clone, Default)]
struct RegistrationState {
    registrations: HashMap<String, Vec<CompiledWatcher>>,
}

impl RegistrationState {
    fn register(
        &mut self,
        registrations: Vec<WatchedFileRegistration>,
        workspace_root: &Path,
    ) -> Result<()> {
        let mut compiled = Vec::new();

        for registration in registrations {
            let watchers = registration
                .options
                .watchers
                .iter()
                .map(|watcher| compile_watcher(watcher, workspace_root))
                .collect::<Result<Vec<_>>>()?;
            compiled.push((registration.id, watchers));
        }

        for (id, watchers) in compiled {
            self.registrations.insert(id, watchers);
        }
        Ok(())
    }

    fn unregister(&mut self, registration_ids: &[String]) {
        for registration_id in registration_ids {
            self.registrations.remove(registration_id);
        }
    }

    fn desired_roots(&self) -> HashSet<PathBuf> {
        self.registrations
            .values()
            .flatten()
            .map(|watcher| watcher.root.clone())
            .collect()
    }

    fn matching_events(&self, event: &Event) -> Vec<FileEvent> {
        let mut deduplicated = HashMap::<(String, u32), FileEvent>::new();

        for (path, change) in event_changes(event) {
            if !self
                .registrations
                .values()
                .flatten()
                .any(|watcher| watcher.matches(&path, change))
            {
                continue;
            }
            let Ok(uri) = Url::from_file_path(&path) else {
                continue;
            };
            deduplicated.insert(
                (uri.to_string(), file_change_code(change)),
                FileEvent::new(uri, change),
            );
        }

        deduplicated.into_values().collect()
    }
}

#[derive(Debug)]
pub(crate) enum WatcherCommand {
    Register {
        registrations: Vec<WatchedFileRegistration>,
        reply: SyncSender<std::result::Result<(), WatcherRequestError>>,
    },
    #[allow(dead_code)] // Constructed by dynamic-registration routing in Task 5.
    Unregister {
        registration_ids: Vec<String>,
        reply: SyncSender<std::result::Result<(), WatcherRequestError>>,
    },
    Shutdown,
    Event(notify::Result<Event>),
}

#[cfg(test)]
type RuntimeWatcher = PollWatcher;
#[cfg(not(test))]
type RuntimeWatcher = RecommendedWatcher;

#[cfg(test)]
fn create_runtime_watcher(event_tx: Sender<WatcherCommand>) -> Result<RuntimeWatcher> {
    // Native filesystem backends can be unavailable inside sandboxed test
    // runners; polling keeps the real-filesystem worker test deterministic.
    Ok(PollWatcher::new(
        move |event| {
            let _ = event_tx.send(WatcherCommand::Event(event));
        },
        Config::default().with_poll_interval(Duration::from_millis(100)),
    )?)
}

#[cfg(not(test))]
fn create_runtime_watcher(event_tx: Sender<WatcherCommand>) -> Result<RuntimeWatcher> {
    Ok(notify::recommended_watcher(move |event| {
        let _ = event_tx.send(WatcherCommand::Event(event));
    })?)
}

fn file_change_code(change: FileChangeType) -> u32 {
    if change == FileChangeType::CREATED {
        1
    } else if change == FileChangeType::CHANGED {
        2
    } else {
        3
    }
}

fn normalized_match_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Prefix(prefix) => Some(prefix.as_os_str().to_string_lossy().into_owned()),
            Component::RootDir => Some(String::new()),
            Component::CurDir => None,
            Component::ParentDir => Some("..".to_string()),
            Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn watch_kind_allows(kind: WatchKind, change: FileChangeType) -> bool {
    match change {
        FileChangeType::CREATED => kind.contains(WatchKind::Create),
        FileChangeType::CHANGED => kind.contains(WatchKind::Change),
        FileChangeType::DELETED => kind.contains(WatchKind::Delete),
        _ => false,
    }
}

fn contains_glob_meta(segment: &str) -> bool {
    segment
        .bytes()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b'{'))
}

fn absolute_pattern_root(pattern: &str) -> PathBuf {
    let path = Path::new(pattern);
    let mut prefix = PathBuf::new();

    for component in path.components() {
        let text = component.as_os_str().to_string_lossy();
        if contains_glob_meta(&text) {
            break;
        }
        prefix.push(component.as_os_str());
    }

    if prefix == path {
        prefix.parent().unwrap_or(Path::new("/")).to_path_buf()
    } else if prefix.as_os_str().is_empty() {
        PathBuf::from("/")
    } else {
        prefix
    }
}

fn compile_glob(pattern: &str) -> Result<GlobMatcher> {
    Ok(GlobBuilder::new(pattern)
        .literal_separator(true)
        .backslash_escape(false)
        .build()
        .with_context(|| format!("invalid LSP glob pattern: {pattern}"))?
        .compile_matcher())
}

fn relative_pattern_base(pattern: &RelativePattern) -> Result<PathBuf> {
    let uri = match &pattern.base_uri {
        OneOf::Left(folder) => &folder.uri,
        OneOf::Right(uri) => uri,
    };
    uri.to_file_path()
        .map_err(|_| anyhow!("watched-file base URI is not a file URI: {uri}"))
}

pub(crate) fn parse_register_params(
    params: Option<serde_json::Value>,
) -> std::result::Result<Vec<WatchedFileRegistration>, WatcherRequestError> {
    let params: RegistrationParams = serde_json::from_value(params.ok_or_else(|| {
        WatcherRequestError::InvalidParams("missing registration params".to_string())
    })?)
    .map_err(|error| WatcherRequestError::InvalidParams(error.to_string()))?;

    params
        .registrations
        .into_iter()
        .filter(|registration| registration.method == WATCHED_FILES_METHOD)
        .map(|registration| {
            let options =
                serde_json::from_value(registration.register_options.ok_or_else(|| {
                    WatcherRequestError::InvalidParams(format!(
                        "watched-file registration {} has no options",
                        registration.id
                    ))
                })?)
                .map_err(|error| WatcherRequestError::InvalidParams(error.to_string()))?;
            Ok(WatchedFileRegistration {
                id: registration.id,
                options,
            })
        })
        .collect()
}

pub(crate) fn parse_unregister_params(
    params: Option<serde_json::Value>,
) -> std::result::Result<Vec<String>, WatcherRequestError> {
    let params: UnregistrationParams = serde_json::from_value(params.ok_or_else(|| {
        WatcherRequestError::InvalidParams("missing unregistration params".to_string())
    })?)
    .map_err(|error| WatcherRequestError::InvalidParams(error.to_string()))?;

    Ok(params
        .unregisterations
        .into_iter()
        .filter(|registration| registration.method == WATCHED_FILES_METHOD)
        .map(|registration| registration.id)
        .collect())
}

fn compile_watcher(
    watcher: &lsp_types::FileSystemWatcher,
    workspace_root: &Path,
) -> Result<CompiledWatcher> {
    let kind = watcher
        .kind
        .unwrap_or(WatchKind::Create | WatchKind::Change | WatchKind::Delete);

    match &watcher.glob_pattern {
        GlobPattern::Relative(pattern) => {
            let base = relative_pattern_base(pattern)?;
            Ok(CompiledWatcher {
                root: base.clone(),
                base,
                matcher: compile_glob(&pattern.pattern)?,
                input: MatchInput::Relative,
                kind,
            })
        }
        GlobPattern::String(pattern) if Path::new(pattern).is_absolute() => Ok(CompiledWatcher {
            base: PathBuf::from("/"),
            root: absolute_pattern_root(pattern),
            matcher: compile_glob(pattern)?,
            input: MatchInput::Absolute,
            kind,
        }),
        GlobPattern::String(pattern) => Ok(CompiledWatcher {
            base: workspace_root.to_path_buf(),
            root: workspace_root.to_path_buf(),
            matcher: compile_glob(pattern)?,
            input: MatchInput::Relative,
            kind,
        }),
    }
}

impl CompiledWatcher {
    fn matches(&self, path: &Path, change: FileChangeType) -> bool {
        if !watch_kind_allows(self.kind, change) {
            return false;
        }

        let candidate = match self.input {
            MatchInput::Relative => match path.strip_prefix(&self.base) {
                Ok(relative) => normalized_match_path(relative),
                Err(_) => return false,
            },
            MatchInput::Absolute => normalized_match_path(path),
        };

        self.matcher.is_match(candidate)
    }
}

fn event_changes(event: &Event) -> Vec<(PathBuf, FileChangeType)> {
    match &event.kind {
        EventKind::Create(_) => event
            .paths
            .iter()
            .cloned()
            .map(|path| (path, FileChangeType::CREATED))
            .collect(),
        EventKind::Remove(_) => event
            .paths
            .iter()
            .cloned()
            .map(|path| (path, FileChangeType::DELETED))
            .collect(),
        EventKind::Modify(ModifyKind::Name(RenameMode::Both)) => match event.paths.as_slice() {
            [from, to] => vec![
                (from.clone(), FileChangeType::DELETED),
                (to.clone(), FileChangeType::CREATED),
            ],
            _ => Vec::new(),
        },
        EventKind::Modify(ModifyKind::Name(RenameMode::From)) => event
            .paths
            .iter()
            .cloned()
            .map(|path| (path, FileChangeType::DELETED))
            .collect(),
        EventKind::Modify(ModifyKind::Name(RenameMode::To)) => event
            .paths
            .iter()
            .cloned()
            .map(|path| (path, FileChangeType::CREATED))
            .collect(),
        EventKind::Modify(_) => event
            .paths
            .iter()
            .cloned()
            .map(|path| (path, FileChangeType::CHANGED))
            .collect(),
        _ => Vec::new(),
    }
}

fn build_notification(changes: Vec<FileEvent>) -> Result<String> {
    let body = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "method": WATCHED_FILES_METHOD,
        "params": DidChangeWatchedFilesParams { changes },
    }))?;
    Ok(format!("Content-Length: {}\r\n\r\n{}", body.len(), body))
}

fn write_notification(stdin: &SharedStdin, changes: Vec<FileEvent>) -> Result<()> {
    let message = build_notification(changes)?;
    let mut stdin = stdin
        .lock()
        .map_err(|_| anyhow!("failed to lock LSP stdin"))?;
    stdin.write_all(message.as_bytes())?;
    stdin.flush()?;
    Ok(())
}

type EventSink = Box<dyn Fn(Vec<FileEvent>) -> Result<()> + Send>;
type ErrorSink = Box<dyn Fn(String) + Send>;

#[allow(dead_code)] // Started by the LSP client wiring in Task 5.
pub(crate) struct WatchedFilesHandle {
    tx: Sender<WatcherCommand>,
    join: Option<JoinHandle<()>>,
}

#[allow(dead_code)] // Methods are wired into the LSP client in Task 5.
impl WatchedFilesHandle {
    #[allow(dead_code)] // Started by the LSP client wiring in Task 5.
    pub(crate) fn start(
        workspace_root: PathBuf,
        stdin: SharedStdin,
        notification_tx: Sender<LspNotification>,
    ) -> Result<Self> {
        let notification_tx_for_write = notification_tx.clone();
        Self::start_with_sink(
            workspace_root,
            Box::new(move |changes| {
                write_notification(&stdin, changes).map_err(|error| {
                    let _ = notification_tx_for_write.send(LspNotification::Error {
                        message: format!("Failed to send watched-file notification: {error}"),
                    });
                    error
                })
            }),
            Box::new(move |message| {
                let _ = notification_tx.send(LspNotification::Error { message });
            }),
        )
    }

    fn start_with_sink(
        workspace_root: PathBuf,
        sink: EventSink,
        error_sink: ErrorSink,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel();
        let event_tx = tx.clone();
        let watcher = create_runtime_watcher(event_tx)?;
        let join = thread::spawn(move || {
            run_worker(workspace_root, rx, watcher, sink, error_sink);
        });
        Ok(Self {
            tx,
            join: Some(join),
        })
    }

    #[allow(dead_code)] // Used by dynamic-registration routing in Task 5.
    pub(crate) fn command_sender(&self) -> Sender<WatcherCommand> {
        self.tx.clone()
    }

    fn request(
        &self,
        build: impl FnOnce(SyncSender<std::result::Result<(), WatcherRequestError>>) -> WatcherCommand,
    ) -> Result<()> {
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.tx.send(build(reply_tx))?;
        reply_rx
            .recv_timeout(Duration::from_secs(2))
            .map_err(|error| anyhow!("watcher worker did not reply: {error}"))?
            .map_err(|error| anyhow!(error.to_string()))
    }

    fn register(&self, registrations: Vec<WatchedFileRegistration>) -> Result<()> {
        self.request(|reply| WatcherCommand::Register {
            registrations,
            reply,
        })
    }

    pub(crate) fn shutdown(&mut self) {
        let _ = self.tx.send(WatcherCommand::Shutdown);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn run_worker(
    workspace_root: PathBuf,
    rx: Receiver<WatcherCommand>,
    mut watcher: RuntimeWatcher,
    sink: EventSink,
    error_sink: ErrorSink,
) {
    let mut state = RegistrationState::default();
    let mut watched_roots = HashSet::new();
    let mut pending = HashMap::<(String, u32), FileEvent>::new();
    let mut flush_at: Option<Instant> = None;

    loop {
        let timeout = flush_at
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(3600));

        match rx.recv_timeout(timeout) {
            Ok(WatcherCommand::Register {
                registrations,
                reply,
            }) => {
                let mut next_state = state.clone();
                let result = next_state
                    .register(registrations, &workspace_root)
                    .and_then(|_| {
                        sync_roots(&mut watcher, &mut watched_roots, next_state.desired_roots())
                    })
                    .map_err(|error| WatcherRequestError::Setup(error.to_string()));
                if result.is_ok() {
                    state = next_state;
                }
                let _ = reply.send(result);
            }
            Ok(WatcherCommand::Unregister {
                registration_ids,
                reply,
            }) => {
                let mut next_state = state.clone();
                next_state.unregister(&registration_ids);
                let result =
                    sync_roots(&mut watcher, &mut watched_roots, next_state.desired_roots())
                        .map_err(|error| WatcherRequestError::Setup(error.to_string()));
                if result.is_ok() {
                    state = next_state;
                }
                let _ = reply.send(result);
            }
            Ok(WatcherCommand::Shutdown) => break,
            Ok(WatcherCommand::Event(Ok(event))) => {
                for change in state.matching_events(&event) {
                    pending.insert(
                        (change.uri.to_string(), file_change_code(change.typ)),
                        change,
                    );
                }
                if !pending.is_empty() && flush_at.is_none() {
                    flush_at = Some(Instant::now() + Duration::from_millis(50));
                }
            }
            Ok(WatcherCommand::Event(Err(error))) => {
                error_sink(format!("LSP file watcher error: {error}"));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !pending.is_empty() {
                    let changes = pending.drain().map(|(_, event)| event).collect();
                    let _ = sink(changes);
                }
                flush_at = None;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn sync_roots(
    watcher: &mut RuntimeWatcher,
    watched_roots: &mut HashSet<PathBuf>,
    desired_roots: HashSet<PathBuf>,
) -> Result<()> {
    let additions = desired_roots
        .difference(watched_roots)
        .cloned()
        .collect::<Vec<_>>();
    let removals = watched_roots
        .difference(&desired_roots)
        .cloned()
        .collect::<Vec<_>>();
    let mut added: Vec<PathBuf> = Vec::new();

    for root in &additions {
        if let Err(error) = watcher.watch(root, RecursiveMode::Recursive) {
            for added_root in &added {
                let _ = watcher.unwatch(added_root);
            }
            return Err(anyhow!("failed to watch {}: {error}", root.display()));
        }
        added.push(root.clone());
    }

    let mut removed: Vec<PathBuf> = Vec::new();
    for root in &removals {
        if let Err(error) = watcher.unwatch(root) {
            for removed_root in &removed {
                let _ = watcher.watch(removed_root, RecursiveMode::Recursive);
            }
            for added_root in &added {
                let _ = watcher.unwatch(added_root);
            }
            return Err(anyhow!("failed to unwatch {}: {error}", root.display()));
        }
        removed.push(root.clone());
    }

    *watched_roots = desired_roots;
    Ok(())
}

// Task 5 wires these staged APIs into the LSP client and request router.
const _: fn(
    Option<serde_json::Value>,
) -> std::result::Result<Vec<WatchedFileRegistration>, WatcherRequestError> = parse_register_params;
const _: fn(Option<serde_json::Value>) -> std::result::Result<Vec<String>, WatcherRequestError> =
    parse_unregister_params;
const _: fn(&lsp_types::FileSystemWatcher, &Path) -> Result<CompiledWatcher> = compile_watcher;
const _: fn(&CompiledWatcher, &Path, FileChangeType) -> bool = CompiledWatcher::matches;
const _: fn(&Event) -> Vec<(PathBuf, FileChangeType)> = event_changes;
const _: fn(Vec<FileEvent>) -> Result<String> = build_notification;
const _: fn(&SharedStdin, Vec<FileEvent>) -> Result<()> = write_notification;

#[cfg(test)]
mod tests {
    use super::*;
    use lsp_types::{
        FileSystemWatcher, Registration, RegistrationParams, Unregistration, UnregistrationParams,
        WorkspaceFolder,
    };
    use serde_json::json;

    fn workspace_folder(path: &Path) -> WorkspaceFolder {
        WorkspaceFolder {
            uri: Url::from_file_path(path).expect("workspace URI"),
            name: "workspace".to_string(),
        }
    }

    fn register_params(registrations: Vec<Registration>) -> Option<serde_json::Value> {
        Some(serde_json::to_value(RegistrationParams { registrations }).unwrap())
    }

    fn unregister_params(unregisterations: Vec<Unregistration>) -> Option<serde_json::Value> {
        Some(serde_json::to_value(UnregistrationParams { unregisterations }).unwrap())
    }

    fn watched_file_registration(id: &str, register_options: serde_json::Value) -> Registration {
        Registration {
            id: id.to_string(),
            method: WATCHED_FILES_METHOD.to_string(),
            register_options: Some(register_options),
        }
    }

    fn watched_registration(id: &str, root: &Path, pattern: &str) -> WatchedFileRegistration {
        WatchedFileRegistration {
            id: id.to_string(),
            options: DidChangeWatchedFilesRegistrationOptions {
                watchers: vec![lsp_types::FileSystemWatcher {
                    glob_pattern: GlobPattern::Relative(RelativePattern {
                        base_uri: OneOf::Right(Url::from_file_path(root).expect("root URI")),
                        pattern: pattern.to_string(),
                    }),
                    kind: None,
                }],
            },
        }
    }

    #[test]
    fn registration_state_adds_and_removes_registration_by_id() {
        let root = PathBuf::from("/tmp/nevi-watch-root");
        let mut state = RegistrationState::default();

        state
            .register(
                vec![watched_registration("rust-files", &root, "**/*.rs")],
                &root,
            )
            .expect("register");
        assert!(state.registrations.contains_key("rust-files"));
        assert_eq!(state.desired_roots(), HashSet::from([root.clone()]));

        state.unregister(&["rust-files".to_string()]);
        assert!(!state.registrations.contains_key("rust-files"));
        assert!(state.desired_roots().is_empty());
    }

    #[test]
    fn worker_emits_matching_changes_and_ignores_unrelated_files() {
        static NEXT_DIR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

        let root = std::env::temp_dir().join(format!(
            "nevi-watched-files-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(root.join("src")).unwrap();

        let (events_tx, events_rx) = mpsc::channel();
        let mut handle = WatchedFilesHandle::start_with_sink(
            root.clone(),
            Box::new(move |changes| {
                events_tx
                    .send(changes)
                    .map_err(|error| anyhow!(error.to_string()))
            }),
            Box::new(|_| {}),
        )
        .expect("watcher");

        handle
            .register(vec![watched_registration("rust-files", &root, "**/*.rs")])
            .expect("register");

        std::fs::write(root.join("README.md"), "ignored").unwrap();
        std::fs::write(root.join("src/main.rs"), "fn main() {}").unwrap();

        let changes = events_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("matching filesystem notification");
        assert!(changes.iter().any(|event| {
            event.uri.to_file_path().ok().as_deref() == Some(root.join("src/main.rs").as_path())
        }));
        assert!(!changes.iter().any(|event| {
            event.uri.to_file_path().ok().as_deref() == Some(root.join("README.md").as_path())
        }));

        handle.shutdown();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn parse_register_params_returns_watched_file_registration_options() {
        let params = register_params(vec![watched_file_registration(
            "rust-files",
            json!({
                "watchers": [
                    {
                        "globPattern": "**/*.rs",
                        "kind": 3
                    }
                ]
            }),
        )]);

        let registrations = parse_register_params(params).expect("registration params");

        assert_eq!(registrations.len(), 1);
        assert_eq!(registrations[0].id, "rust-files");
        assert_eq!(registrations[0].options.watchers.len(), 1);
        assert_eq!(
            registrations[0].options.watchers[0].glob_pattern,
            GlobPattern::String("**/*.rs".to_string())
        );
        assert_eq!(
            registrations[0].options.watchers[0].kind,
            Some(WatchKind::Create | WatchKind::Change)
        );
    }

    #[test]
    fn parse_register_params_filters_unrelated_registration_methods() {
        let params = register_params(vec![Registration {
            id: "configuration".to_string(),
            method: "workspace/didChangeConfiguration".to_string(),
            register_options: None,
        }]);

        let registrations = parse_register_params(params).expect("registration params");

        assert!(registrations.is_empty());
    }

    #[test]
    fn parse_register_params_rejects_missing_params_or_malformed_watched_file_options() {
        assert!(matches!(
            parse_register_params(None),
            Err(WatcherRequestError::InvalidParams(message))
                if message == "missing registration params"
        ));

        let malformed = register_params(vec![watched_file_registration(
            "rust-files",
            json!({ "watchers": "not an array" }),
        )]);

        assert!(matches!(
            parse_register_params(malformed),
            Err(WatcherRequestError::InvalidParams(_))
        ));
    }

    #[test]
    fn parse_unregister_params_filters_to_watched_file_registration_ids() {
        let params = unregister_params(vec![
            Unregistration {
                id: "rust-files".to_string(),
                method: WATCHED_FILES_METHOD.to_string(),
            },
            Unregistration {
                id: "configuration".to_string(),
                method: "workspace/didChangeConfiguration".to_string(),
            },
        ]);

        let registration_ids = parse_unregister_params(params).expect("unregistration params");

        assert_eq!(registration_ids, vec!["rust-files".to_string()]);
    }

    #[test]
    fn relative_pattern_matches_only_under_its_base_uri() {
        let root = PathBuf::from("/tmp/nevi-watch-root");
        let watcher = FileSystemWatcher {
            glob_pattern: GlobPattern::Relative(RelativePattern {
                base_uri: OneOf::Left(workspace_folder(&root)),
                pattern: "**/*.rs".to_string(),
            }),
            kind: None,
        };

        let compiled = compile_watcher(&watcher, &root).expect("compiled watcher");

        assert!(compiled.matches(&root.join("src/main.rs"), FileChangeType::CHANGED));
        assert!(!compiled.matches(&root.join("README.md"), FileChangeType::CHANGED));
        assert!(!compiled.matches(Path::new("/tmp/other/src/main.rs"), FileChangeType::CHANGED));
    }

    #[test]
    fn relative_pattern_accepts_base_uri_as_url() {
        let workspace_root = PathBuf::from("/tmp/nevi-watch-workspace");
        let base = PathBuf::from("/tmp/nevi-watch-base");
        let watcher = FileSystemWatcher {
            glob_pattern: GlobPattern::Relative(RelativePattern {
                base_uri: OneOf::Right(Url::from_file_path(&base).expect("base URI")),
                pattern: "src/**/*.rs".to_string(),
            }),
            kind: None,
        };

        let compiled = compile_watcher(&watcher, &workspace_root).expect("compiled watcher");

        assert_eq!(compiled.root, base);
        assert!(compiled.matches(
            Path::new("/tmp/nevi-watch-base/src/nested/lib.rs"),
            FileChangeType::CHANGED
        ));
        assert!(!compiled.matches(
            Path::new("/tmp/nevi-watch-workspace/src/nested/lib.rs"),
            FileChangeType::CHANGED
        ));
    }

    #[test]
    fn string_pattern_uses_workspace_root_when_relative() {
        let root = PathBuf::from("/tmp/nevi-watch-root");
        let watcher = FileSystemWatcher {
            glob_pattern: GlobPattern::String("**/Cargo.{toml,lock}".to_string()),
            kind: None,
        };

        let compiled = compile_watcher(&watcher, &root).expect("compiled watcher");

        assert!(compiled.matches(&root.join("crates/app/Cargo.toml"), FileChangeType::CREATED));
        assert!(compiled.matches(&root.join("Cargo.lock"), FileChangeType::CHANGED));
        assert!(!compiled.matches(&root.join("src/main.rs"), FileChangeType::CHANGED));
    }

    #[test]
    fn absolute_string_pattern_watches_parent_and_matches_exact_file() {
        let root = std::env::temp_dir().join("nevi-watch-root");
        let manifest = root.join("Cargo.toml");
        let watcher = FileSystemWatcher {
            glob_pattern: GlobPattern::String(normalized_match_path(&manifest)),
            kind: None,
        };

        let compiled = compile_watcher(&watcher, &root).expect("compiled watcher");

        assert_eq!(compiled.root, root);
        assert!(compiled.matches(&manifest, FileChangeType::CHANGED));
        assert!(!compiled.matches(&root.join("Cargo.lock"), FileChangeType::CHANGED));
    }

    #[test]
    fn absolute_string_glob_pattern_watches_static_prefix_and_matches_descendants() {
        let root = std::env::temp_dir().join("nevi-watch-root");
        let src = root.join("src");
        let pattern = format!("{}/**/*.rs", normalized_match_path(&src));
        let watcher = FileSystemWatcher {
            glob_pattern: GlobPattern::String(pattern),
            kind: None,
        };

        let compiled = compile_watcher(&watcher, &root).expect("compiled watcher");

        assert_eq!(compiled.root, src);
        assert!(compiled.matches(&root.join("src/nested/lib.rs"), FileChangeType::CHANGED));
        assert!(!compiled.matches(&root.join("tests/nested/lib.rs"), FileChangeType::CHANGED));
        assert!(!compiled.matches(&root.join("src/nested/lib.toml"), FileChangeType::CHANGED));
    }

    #[test]
    fn watcher_kind_filters_unrequested_events() {
        let root = PathBuf::from("/tmp/nevi-watch-root");
        let watcher = FileSystemWatcher {
            glob_pattern: GlobPattern::String("**/*.rs".to_string()),
            kind: Some(WatchKind::Create | WatchKind::Delete),
        };

        let compiled = compile_watcher(&watcher, &root).expect("compiled watcher");
        let path = root.join("src/lib.rs");

        assert!(compiled.matches(&path, FileChangeType::CREATED));
        assert!(!compiled.matches(&path, FileChangeType::CHANGED));
        assert!(compiled.matches(&path, FileChangeType::DELETED));
    }

    #[test]
    fn rename_event_becomes_delete_then_create() {
        let event = Event {
            kind: EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
            paths: vec![
                PathBuf::from("/tmp/nevi-watch-root/src/old.rs"),
                PathBuf::from("/tmp/nevi-watch-root/src/new.rs"),
            ],
            attrs: Default::default(),
        };

        assert_eq!(
            event_changes(&event),
            vec![
                (
                    PathBuf::from("/tmp/nevi-watch-root/src/old.rs"),
                    FileChangeType::DELETED,
                ),
                (
                    PathBuf::from("/tmp/nevi-watch-root/src/new.rs"),
                    FileChangeType::CREATED,
                ),
            ]
        );
    }

    #[test]
    fn rename_both_with_unexpected_path_count_is_ignored() {
        let malformed_paths = [
            vec![],
            vec![PathBuf::from("/tmp/nevi-watch-root/src/only.rs")],
            vec![
                PathBuf::from("/tmp/nevi-watch-root/src/old.rs"),
                PathBuf::from("/tmp/nevi-watch-root/src/new.rs"),
                PathBuf::from("/tmp/nevi-watch-root/src/extra.rs"),
            ],
        ];

        for paths in malformed_paths {
            let event = Event {
                kind: EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                paths,
                attrs: Default::default(),
            };

            assert_eq!(event_changes(&event), Vec::new());
        }
    }

    #[test]
    fn did_change_watched_files_message_is_valid_json_rpc() {
        let changes = vec![FileEvent::new(
            Url::parse("file:///tmp/nevi-watch-root/src/main.rs").unwrap(),
            FileChangeType::CHANGED,
        )];

        let message = build_notification(changes).expect("notification");
        let (_, body) = message.split_once("\r\n\r\n").expect("framed message");
        let body: serde_json::Value = serde_json::from_str(body).expect("JSON body");

        assert_eq!(body["method"], WATCHED_FILES_METHOD);
        assert_eq!(body["params"]["changes"][0]["type"], 2);
    }
}
