//! Owns the process-wide match registry: every concurrently-running match's
//! channels, keyed by id, plus the shared lobby feed the homepage's
//! browsable list subscribes to.
//!
//! One `battle::run_match` thread per match, same shape `battle.rs` already
//! used for its single match — this module just decides *when* to spawn one
//! (on `create_match`, or once per unfinished match found on disk at
//! `bootstrap`) and keeps track of the resulting channels so `web.rs` can
//! route a request to the right match by id.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::battle;
use crate::domain::{Battle, BattleStatus, MatchSummary, Scoreboard, TeamStats};
use crate::protocol::ClientMsg;

/// A running match's channels, cloned out of the registry for whichever
/// request needs them — `mpsc::Sender`/`watch::Receiver` are both cheap to
/// clone, so callers never hold the registry's lock while using them.
#[derive(Clone)]
pub struct MatchHandle {
    pub msg_tx: mpsc::Sender<ClientMsg>,
    pub state_rx: watch::Receiver<Scoreboard>,
    pub host_token: String,
}

/// The process-wide set of matches. Wrapped in `Arc` so axum's `State`
/// extractor and every match's spawning thread can share it cheaply.
pub struct Registry {
    data_dir: PathBuf,
    next_id: AtomicU64,
    matches: Mutex<HashMap<u64, MatchHandle>>,
    lobby: Arc<watch::Sender<HashMap<u64, MatchSummary>>>,
}

impl Registry {
    /// Resumes every match on disk that hadn't ended when the process last
    /// stopped (see `battle::find_all_resumable_battles`), then returns a
    /// registry ready to also serve brand-new matches — `id`s always start
    /// past the highest one already on disk, so a freshly created match
    /// can never collide with a resumed one.
    pub fn bootstrap(data_dir: PathBuf) -> Arc<Registry> {
        std::fs::create_dir_all(&data_dir).expect("create MMO_DATA_DIR");

        let next_id = battle::max_existing_battle_id(&data_dir) + 1;
        let (lobby_tx, _lobby_rx) = watch::channel(HashMap::new());
        let registry = Arc::new(Registry {
            data_dir,
            next_id: AtomicU64::new(next_id),
            matches: Mutex::new(HashMap::new()),
            lobby: Arc::new(lobby_tx),
        });

        for (id, battle) in battle::find_all_resumable_battles(&registry.data_dir) {
            let host_token = battle.host_token.clone();
            registry.spawn_match(id, host_token, Some(battle));
        }

        registry
    }

    /// Allocates a fresh id and host token, spawns the thread that'll run
    /// this match end to end, and registers its channels. Returns the pair
    /// the caller needs to build the two links: `/match/{id}` (share with
    /// players) and `/match/{id}/host/{token}` (keep for the host).
    pub fn create_match(&self) -> (u64, String) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let host_token = generate_host_token();
        self.spawn_match(id, host_token.clone(), None);
        (id, host_token)
    }

    /// Looks up a match's channels by id, for the player/host page and
    /// websocket handlers in `web.rs`.
    pub fn get(&self, id: u64) -> Option<MatchHandle> {
        self.matches.lock().unwrap().get(&id).cloned()
    }

    /// A fresh receiver onto the shared lobby map, for the homepage's
    /// websocket to forward live to its browser.
    pub fn subscribe_lobby(&self) -> watch::Receiver<HashMap<u64, MatchSummary>> {
        self.lobby.subscribe()
    }

    fn spawn_match(&self, id: u64, host_token: String, resume: Option<Battle>) {
        let battle_dir = self.data_dir.join(format!("battle-{id}"));
        let (msg_tx, msg_rx) = mpsc::channel();
        let (state_tx, state_rx) = watch::channel(placeholder_scoreboard(id, &host_token));
        let lobby = self.lobby.clone();
        let thread_token = host_token.clone();
        std::thread::spawn(move || {
            battle::run_match(
                battle_dir,
                id,
                thread_token,
                resume,
                msg_rx,
                state_tx,
                lobby,
            )
        });
        self.matches.lock().unwrap().insert(
            id,
            MatchHandle {
                msg_tx,
                state_rx,
                host_token,
            },
        );
    }
}

/// 128 bits of randomness, hex-encoded — unguessable enough to gate
/// host-only actions given this app's existing lightweight-trust posture
/// (player identity is likewise just a client-generated id, see `web.rs`).
fn generate_host_token() -> String {
    let bytes: [u8; 16] = rand::random();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Sent to no one — overwritten by `run_match`'s first snapshot before any
/// websocket client can possibly connect. Exists only because
/// `watch::channel` needs an initial value.
fn placeholder_scoreboard(id: u64, host_token: &str) -> Scoreboard {
    Scoreboard {
        battle: Battle {
            id,
            host_token: host_token.to_string(),
            park: None,
            duration_ms: None,
            status: BattleStatus::Pending,
            started_at_ms: None,
            ends_at_ms: None,
            outcome: None,
        },
        court_square: TeamStats::default(),
        church_ave: TeamStats::default(),
    }
}
