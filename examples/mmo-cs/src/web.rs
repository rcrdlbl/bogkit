//! HTTP + websocket surface. Three kinds of page, all inline HTML/JS
//! constants — no static files, no template engine, matching `examples/chat`'s
//! convention:
//!
//!  - `/` — the homepage: a "host a new match" button and a live, browsable
//!    list of running (and recently-ended) matches, fed by `/lobby/ws`.
//!  - `/match/:id` — the player link: join a team, stream location once the
//!    battle's active. Shared freely — it carries no special privilege.
//!  - `/match/:id/host/:token` — the host link: configure the battleground +
//!    match length and start the battle, but never join a team. `:token` is
//!    checked against the match's `host_token`; get it wrong and you get the
//!    same 404 as a nonexistent match, so a bad guess can't even confirm the
//!    match exists.
//!
//! Both match pages share one template (`MATCH_PAGE_TEMPLATE`), parameterized
//! by whether the viewer is the host — the host's own websocket connection
//! carries the token (`/match/:id/ws?host=...`), which is what actually gates
//! `ConfigureBattle`/`StartBattle` in [`handle_socket`]; the UI just hides
//! those controls from non-hosts as a second, non-load-bearing layer.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc;

use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use qrcode::QrCode;
use qrcode::render::svg;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::domain::{MatchSummary, Scoreboard};
use crate::matches::Registry;
use crate::parks;
use crate::protocol::ClientMsg;

type AppState = Arc<Registry>;

#[tokio::main]
pub async fn serve(registry: Arc<Registry>) {
    let app = Router::new()
        .route("/", get(lobby_page))
        .route("/lobby/ws", get(lobby_ws_upgrade))
        .route("/matches", post(create_match))
        .route("/match/{id}", get(match_page))
        .route("/match/{id}/host/{token}", get(match_host_page))
        .route("/match/{id}/ws", get(match_ws_upgrade))
        .route("/qr", get(qr_code))
        .with_state(registry);

    let port: u16 = std::env::var("MMO_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);
    let addr = format!("0.0.0.0:{port}");
    println!("area denial running on http://localhost:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[derive(Serialize)]
struct CreateMatchResponse {
    id: u64,
    host_token: String,
}

async fn create_match(State(registry): State<AppState>) -> Json<CreateMatchResponse> {
    let (id, host_token) = registry.create_match();
    Json(CreateMatchResponse { id, host_token })
}

async fn match_page(State(registry): State<AppState>, Path(id): Path<u64>) -> Response {
    if registry.get(id).is_none() {
        return (StatusCode::NOT_FOUND, "no such match").into_response();
    }
    Html(render_match_page(id, false, "")).into_response()
}

async fn match_host_page(
    State(registry): State<AppState>,
    Path((id, token)): Path<(u64, String)>,
) -> Response {
    match registry.get(id) {
        Some(handle) if handle.host_token == token => {
            Html(render_match_page(id, true, &token)).into_response()
        }
        // Deliberately the same response whether the match doesn't exist or
        // the token is just wrong — a bad guess shouldn't be able to
        // distinguish the two.
        _ => (StatusCode::NOT_FOUND, "no such match").into_response(),
    }
}

fn render_match_page(id: u64, is_host: bool, host_token: &str) -> String {
    let parks_json = serde_json::to_string(parks::all()).unwrap_or_else(|_| "[]".to_string());
    MATCH_PAGE_TEMPLATE
        .replacen("__PARKS_JSON__", &parks_json, 1)
        .replacen("__MATCH_ID__", &id.to_string(), 1)
        .replacen("__IS_HOST__", &is_host.to_string(), 1)
        .replacen("__HOST_TOKEN__", host_token, 1)
}

#[derive(Deserialize)]
struct MatchWsParams {
    host: Option<String>,
}

async fn match_ws_upgrade(
    State(registry): State<AppState>,
    Path(id): Path<u64>,
    Query(params): Query<MatchWsParams>,
    ws: WebSocketUpgrade,
) -> Response {
    let Some(handle) = registry.get(id) else {
        return (StatusCode::NOT_FOUND, "no such match").into_response();
    };
    // Only a connection presenting the exact host token may later send
    // ConfigureBattle/StartBattle — see handle_socket. Everyone else is
    // treated as a plain player connection.
    let is_host = params.host.as_deref() == Some(handle.host_token.as_str());
    ws.on_upgrade(move |socket| handle_socket(socket, handle.msg_tx, handle.state_rx, is_host))
}

/// Per-client task: push every new scoreboard down, feed every incoming
/// client message into the match's ingest thread.
async fn handle_socket(
    mut socket: WebSocket,
    msg_tx: mpsc::Sender<ClientMsg>,
    mut state_rx: watch::Receiver<Scoreboard>,
    is_host: bool,
) {
    let state_json = |s: &Scoreboard| serde_json::to_string(s).unwrap();

    let hello = state_json(&state_rx.borrow_and_update());
    if socket.send(Message::text(hello)).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            changed = state_rx.changed() => {
                if changed.is_err() {
                    return; // match thread gone
                }
                let update = state_json(&state_rx.borrow_and_update());
                if socket.send(Message::text(update)).await.is_err() {
                    return;
                }
            }
            incoming = socket.recv() => {
                let Some(Ok(Message::Text(text))) = incoming else {
                    return; // client closed or errored
                };
                match serde_json::from_str::<ClientMsg>(&text) {
                    Ok(msg) => {
                        let host_only = matches!(
                            msg,
                            ClientMsg::ConfigureBattle { .. } | ClientMsg::StartBattle
                        );
                        if host_only && !is_host {
                            eprintln!("dropping host-only message from a non-host connection");
                        } else if msg_tx.send(msg).is_err() {
                            return; // match thread gone
                        }
                    }
                    Err(e) => eprintln!("dropping malformed client message: {e}"),
                }
            }
        }
    }
}

async fn lobby_page() -> Html<&'static str> {
    Html(LOBBY_PAGE_TEMPLATE)
}

async fn lobby_ws_upgrade(State(registry): State<AppState>, ws: WebSocketUpgrade) -> Response {
    let rx = registry.subscribe_lobby();
    ws.on_upgrade(move |socket| handle_lobby_socket(socket, rx))
        .into_response()
}

/// Push-only: forwards the shared match map to the homepage on every
/// change. Still watches for the client closing the socket (rather than
/// only `send` failing after some later change) so an abandoned tab's task
/// doesn't linger until the next unrelated match update.
async fn handle_lobby_socket(
    mut socket: WebSocket,
    mut rx: watch::Receiver<HashMap<u64, MatchSummary>>,
) {
    let to_json = |m: &HashMap<u64, MatchSummary>| {
        let mut list: Vec<&MatchSummary> = m.values().collect();
        list.sort_by_key(|m| std::cmp::Reverse(m.id)); // newest first
        serde_json::to_string(&list).unwrap()
    };

    let hello = to_json(&rx.borrow_and_update());
    if socket.send(Message::text(hello)).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let update = to_json(&rx.borrow_and_update());
                if socket.send(Message::text(update)).await.is_err() {
                    return;
                }
            }
            incoming = socket.recv() => {
                if incoming.is_none() {
                    return; // client closed
                }
                // this socket is push-only; any incoming frame is ignored
            }
        }
    }
}

#[derive(Deserialize)]
struct QrParams {
    data: String,
}

/// Renders `?data=` as a QR code SVG. Generated server-side (rather than an
/// embedded JS encoder) so the page stays self-contained without vendoring
/// a QR algorithm — and server-generated from `location.origin` client-side
/// means it always encodes whatever URL is actually working for the phone
/// that's viewing it, not a guessed one. Served from the same origin, so it
/// still works with no outside internet access, matching this page's
/// convention of no external requests.
async fn qr_code(Query(params): Query<QrParams>) -> impl IntoResponse {
    let Ok(code) = QrCode::new(params.data.as_bytes()) else {
        return (StatusCode::BAD_REQUEST, "data too long for a QR code").into_response();
    };
    let svg = code
        .render::<svg::Color>()
        .min_dimensions(240, 240)
        .dark_color(svg::Color("#000"))
        .light_color(svg::Color("#fff"))
        .build();
    ([(header::CONTENT_TYPE, "image/svg+xml")], svg).into_response()
}

const LOBBY_PAGE_TEMPLATE: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>area denial</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 32rem; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-size: 1.25rem; }
  #host-btn { width: 100%; padding: 0.75rem; font-size: 1rem; margin: 1rem 0; }
  .match { border: 1px solid #888; border-radius: 6px; padding: 0.75rem; margin-bottom: 0.75rem; }
  .match h3 { margin: 0 0 0.25rem; font-size: 1rem; font-weight: normal; }
  .match .meta { color: #555; font-size: 0.9rem; }
  .match a.join { display: inline-block; margin-top: 0.5rem; }
  .status { display: inline-block; padding: 0.1rem 0.5rem; border-radius: 3px; font-size: 0.75rem; margin-right: 0.4rem; }
  .status.pending { background: #ddd; }
  .status.active { background: #cfe8cf; }
  .status.ended { background: #eee; color: #888; }
  #empty { color: #888; }
</style>
</head>
<body>
<h1>area denial</h1>
<button id="host-btn">host a new match</button>
<div id="matches"></div>
<p id="empty">no matches running right now — host one to get started.</p>
<script>
document.getElementById("host-btn").onclick = async () => {
  const res = await fetch("/matches", { method: "POST" });
  const { id, host_token } = await res.json();
  location.href = `/match/${id}/host/${host_token}`;
};

// Ended matches stay visible for a while so people can see the outcome,
// then quietly drop off the list — computed client-side off `ends_at_ms`
// so no server-side cleanup timer is needed.
const ENDED_GRACE_MS = 10 * 60 * 1000;

let latestMatches = [];

function statusLabel(m) {
  if (m.status === "pending") return "lobby";
  if (m.status === "active") return "active";
  return "ended";
}

function outcomeText(m) {
  if (m.status !== "ended") return "";
  const o = m.outcome;
  if (!o) return "ended";
  if (o.kind === "tie") return "ended in a tie";
  const label = o.winner === "court_square" ? "Team Court Square" : "Team Church Ave";
  const via = o.kind === "elimination" ? "by elimination" : "on time";
  return `${label} won ${via}`;
}

function render() {
  const container = document.getElementById("matches");
  const empty = document.getElementById("empty");
  const now = Date.now();
  const visible = latestMatches.filter(
    (m) => m.status !== "ended" || (m.ends_at_ms && now - m.ends_at_ms < ENDED_GRACE_MS),
  );

  empty.style.display = visible.length === 0 ? "" : "none";
  container.innerHTML = "";
  for (const m of visible) {
    const el = document.createElement("div");
    el.className = "match";
    const park = m.park_name ?? "choosing a battleground…";
    const outcome = outcomeText(m);
    el.innerHTML = `
      <h3><span class="status ${m.status}">${statusLabel(m)}</span>${park}</h3>
      <div class="meta">Court Square ${m.court_square_members} &middot; Church Ave ${m.church_ave_members}${outcome ? " &middot; " + outcome : ""}</div>
      <a class="join" href="/match/${m.id}">${m.status === "ended" ? "view" : "join"}</a>
    `;
    container.appendChild(el);
  }
}

const wsProtocol = location.protocol === "https:" ? "wss:" : "ws:";
const ws = new WebSocket(`${wsProtocol}//${location.host}/lobby/ws`);
ws.onmessage = (event) => {
  latestMatches = JSON.parse(event.data);
  render();
};

// re-check the ended grace window even between server pushes
setInterval(render, 30000);
</script>
</body>
</html>"#;

const MATCH_PAGE_TEMPLATE: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>area denial</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 32rem; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-size: 1.25rem; }
  .teams { display: flex; gap: 1rem; margin: 1rem 0; }
  .team { flex: 1; border: 1px solid #888; border-radius: 6px; padding: 0.75rem; }
  .team button { width: 100%; padding: 0.5rem; font-size: 1rem; }
  .team.mine { border-width: 3px; }
  .bar { background: #ddd; border-radius: 3px; height: 0.6rem; margin-top: 0.4rem; overflow: hidden; }
  .bar > div { background: #2a7; height: 100%; }
  #banner { padding: 0.75rem; border-radius: 6px; margin: 1rem 0; display: none; }
  #banner.show { display: block; }
  #status { color: #555; }
  #my-status { font-weight: bold; margin: 0.5rem 0; }
  #my-status.in { color: #2a7; }
  #my-status.out { color: #c33; }
  button:disabled { opacity: 0.5; }
  #config-screen { border: 1px solid #888; border-radius: 6px; padding: 0.75rem; margin: 1rem 0; }
  #config-screen label { display: block; margin-top: 0.75rem; font-size: 0.9rem; color: #555; }
  #park-search, #duration-select { width: 100%; padding: 0.4rem; font-size: 1rem; box-sizing: border-box; }
  #park-results { max-height: 10rem; overflow-y: auto; margin-top: 0.25rem; }
  .park-result { padding: 0.4rem 0.5rem; border-radius: 4px; cursor: pointer; }
  .park-result:hover { background: #eee; }
  #park-selected { margin-top: 0.5rem; font-weight: bold; }
  #create-match { width: 100%; padding: 0.6rem; font-size: 1rem; margin-top: 1rem; }
  #qr-invite { text-align: center; margin: 1rem 0; }
  #qr-invite img { border: 1px solid #ddd; border-radius: 6px; }
  #qr-invite p { margin: 0 0 0.4rem; color: #555; font-size: 0.9rem; }
  #back-home { color: #555; font-size: 0.9rem; }
</style>
</head>
<body>
<p><a id="back-home" href="/">&larr; all matches</a></p>
<h1>area denial: <span id="park">connecting...</span></h1>
<div id="status"></div>

<div id="config-screen" style="display:none">
  <h3 style="margin-top:0">choose a battleground</h3>
  <input type="text" id="park-search" placeholder="search parks by name..." autocomplete="off">
  <div id="park-results"></div>
  <div id="park-selected"></div>
  <label for="duration-select">match length</label>
  <select id="duration-select">
    <option value="900">15 minutes</option>
    <option value="1800">30 minutes</option>
    <option value="3600">1 hour</option>
    <option value="10800" selected>3 hours</option>
  </select>
  <button id="create-match" disabled>create match</button>
</div>

<div id="my-status"></div>
<div id="banner"></div>
<div id="qr-invite" style="display:none">
  <p>scan to join on your phone</p>
  <img id="qr-img" width="180" height="180" alt="scan to open this match">
</div>
<div class="teams" id="teams">
  <div class="team" id="team-court_square">
    <h3>Team Court Square</h3>
    <div id="members-court_square">0 members</div>
    <div class="bar"><div id="bar-court_square" style="width:0%"></div></div>
    <div id="pct-court_square"></div>
    <button id="join-court_square">join</button>
  </div>
  <div class="team" id="team-church_ave">
    <h3>Team Church Ave</h3>
    <div id="members-church_ave">0 members</div>
    <div class="bar"><div id="bar-church_ave" style="width:0%"></div></div>
    <div id="pct-church_ave"></div>
    <button id="join-church_ave">join</button>
  </div>
</div>
<button id="start" style="display:none">start battle</button>
<p style="color:#888; font-size:0.9rem">
  Location is only sent once a battle is active, and only ever shown as a
  team-wide aggregate — never your teammates' or opponents' positions.
</p>
<script>
const PARKS = __PARKS_JSON__; // [{id, name, bbox}, ...] — the full catalog, embedded so search needs no round trip
const MATCH_ID = __MATCH_ID__;
const IS_HOST = __IS_HOST__; // the host configures + starts the match but never joins a team
const HOST_TOKEN = "__HOST_TOKEN__";
const PLAYER_KEY = `mmo_player_id`;
const TEAM_KEY = `mmo_team_${MATCH_ID}`;

let playerId = localStorage.getItem(PLAYER_KEY);
if (!playerId) {
  playerId = crypto.randomUUID();
  localStorage.setItem(PLAYER_KEY, playerId);
}
let myTeam = IS_HOST ? null : localStorage.getItem(TEAM_KEY); // "court_square" | "church_ave" | null

let latest = null; // last scoreboard from the server
let watchId = null;
let lastPingAt = 0;
let lastFix = null; // { lat, lon } from the most recent geolocation fix

const wsProtocol = location.protocol === "https:" ? "wss:" : "ws:";
const wsUrl = IS_HOST
  ? `${wsProtocol}//${location.host}/match/${MATCH_ID}/ws?host=${encodeURIComponent(HOST_TOKEN)}`
  : `${wsProtocol}//${location.host}/match/${MATCH_ID}/ws`;
const ws = new WebSocket(wsUrl);

function send(msg) { ws.send(JSON.stringify(msg)); }

// The join URL never changes, so this is set once — only #qr-invite's
// visibility toggles per battle status, in render(). Always the plain
// player link, even on the host's own page — the host link is never
// meant to be shared.
document.getElementById("qr-img").src =
  `/qr?data=${encodeURIComponent(location.origin + "/match/" + MATCH_ID)}`;

document.getElementById("join-court_square").onclick = () => join("court_square");
document.getElementById("join-church_ave").onclick = () => join("church_ave");
document.getElementById("start").onclick = () => send({ type: "start_battle" });

function join(team) {
  if (IS_HOST) return; // the host never joins a team
  myTeam = team;
  localStorage.setItem(TEAM_KEY, team);
  send({ type: "join", player: playerId, team });
  render();
}

// --- park search + match-length menu, host-only, shown while battle.park is null ---
let selectedParkId = null;
const parkSearch = document.getElementById("park-search");
const parkResults = document.getElementById("park-results");
const parkSelected = document.getElementById("park-selected");
const durationSelect = document.getElementById("duration-select");
const createMatchBtn = document.getElementById("create-match");

function renderParkResults(query) {
  const q = query.trim().toLowerCase();
  const matches = PARKS.filter((p) => p.name.toLowerCase().includes(q)).slice(0, 8);
  parkResults.innerHTML = "";
  for (const p of matches) {
    const row = document.createElement("div");
    row.className = "park-result";
    row.textContent = p.name;
    row.onclick = () => selectPark(p);
    parkResults.appendChild(row);
  }
}

function selectPark(p) {
  selectedParkId = p.id;
  parkSelected.textContent = `selected: ${p.name}`;
  parkResults.innerHTML = "";
  parkSearch.value = p.name;
  createMatchBtn.disabled = false;
}

parkSearch.oninput = () => {
  selectedParkId = null;
  parkSelected.textContent = "";
  createMatchBtn.disabled = true;
  renderParkResults(parkSearch.value);
};

createMatchBtn.onclick = () => {
  if (!selectedParkId) return;
  send({
    type: "configure_battle",
    park_id: selectedParkId,
    duration_secs: Number(durationSelect.value),
  });
};

if (IS_HOST) renderParkResults(""); // browsable from the start, narrows as you type

function startStreaming() {
  if (IS_HOST || watchId !== null || !navigator.geolocation) return;
  watchId = navigator.geolocation.watchPosition(
    (pos) => {
      const now = Date.now();
      lastFix = { lat: pos.coords.latitude, lon: pos.coords.longitude };
      renderMyStatus(); // update immediately, independent of the ping throttle below
      if (now - lastPingAt < 3000) return; // throttle sends to ~1 ping / 3s
      lastPingAt = now;
      send({
        type: "ping",
        player: playerId,
        lat: lastFix.lat,
        lon: lastFix.lon,
        client_ms: now,
      });
    },
    (err) => console.warn("geolocation error", err),
    { enableHighAccuracy: true, maximumAge: 5000 },
  );
}

function inBounds(bbox, lat, lon) {
  return lon >= bbox.min_lon && lon <= bbox.max_lon && lat >= bbox.min_lat && lat <= bbox.max_lat;
}

// Shown only while actively streaming (on a team, battle active) — never
// applies to the host, who has no team and never streams location.
// Computed entirely client-side from the last GPS fix and the battle's
// bbox (already in every scoreboard) — no extra server round-trip needed.
function renderMyStatus() {
  const el = document.getElementById("my-status");
  if (IS_HOST || !latest || !myTeam || !latest.battle.park || latest.battle.status !== "active") {
    el.textContent = "";
    el.className = "";
    return;
  }
  if (!lastFix) {
    el.textContent = "you: waiting for location...";
    el.className = "";
    return;
  }
  const inside = inBounds(latest.battle.park.bbox, lastFix.lat, lastFix.lon);
  el.textContent = inside ? "you: ✅ inside the battleground" : "you: ❌ outside the battleground";
  el.className = inside ? "in" : "out";
}

function fmtCountdown(ms) {
  if (ms <= 0) return "0:00:00";
  const totalSec = Math.floor(ms / 1000);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function render() {
  if (!latest) return;
  const { battle, court_square, church_ave } = latest;
  const teams = { court_square, church_ave };

  const configScreen = document.getElementById("config-screen");
  const teamsEl = document.getElementById("teams");
  const startBtn = document.getElementById("start");
  const statusEl = document.getElementById("status");
  const banner = document.getElementById("banner");
  const qrInvite = document.getElementById("qr-invite");

  if (!battle.park) {
    // nobody's picked a battleground yet — the host sees the search/duration
    // menu, everyone else just waits for it
    teamsEl.style.display = "none";
    startBtn.style.display = "none";
    qrInvite.style.display = "none";
    banner.classList.remove("show");
    document.getElementById("my-status").textContent = "";
    if (IS_HOST) {
      configScreen.style.display = "";
      document.getElementById("park").textContent = "choose a park";
      statusEl.textContent = "pick a battleground and match length to open the lobby";
    } else {
      configScreen.style.display = "none";
      document.getElementById("park").textContent = "waiting for host...";
      statusEl.textContent = "waiting for the host to choose a battleground and match length";
    }
    return;
  }
  configScreen.style.display = "none"; // locked in once chosen — the host doesn't re-configure mid-lobby
  teamsEl.style.display = "";
  // only useful while players can still join — roster locks once Active
  qrInvite.style.display = battle.status === "pending" ? "" : "none";

  document.getElementById("park").textContent = battle.park.name;

  for (const key of ["court_square", "church_ave"]) {
    const t = teams[key];
    const pct = t.members > 0 ? Math.round((100 * t.in_bounds) / t.members) : 0;
    document.getElementById(`members-${key}`).textContent = `${t.members} members`;
    document.getElementById(`bar-${key}`).style.width = `${pct}%`;
    document.getElementById(`pct-${key}`).textContent =
      battle.status === "pending" ? "" : `${t.in_bounds}/${t.members} in bounds (${pct}%)`;
    document.getElementById(`team-${key}`).classList.toggle("mine", myTeam === key);
    const btn = document.getElementById(`join-${key}`);
    btn.style.display = IS_HOST || myTeam || battle.status !== "pending" ? "none" : "";
  }

  startBtn.style.display = battle.status === "pending" && (IS_HOST || myTeam) ? "" : "none";
  startBtn.disabled = court_square.members === 0 || church_ave.members === 0;

  banner.classList.remove("show");

  if (battle.status === "pending") {
    statusEl.textContent = IS_HOST
      ? "waiting for both teams to have at least one member, then start the battle"
      : myTeam
        ? "waiting for both teams to have at least one member, then the host starts the battle"
        : "pick a team to join";
  } else if (battle.status === "active") {
    const remaining = (battle.ends_at_ms ?? 0) - Date.now();
    statusEl.textContent = `battle active — time remaining ${fmtCountdown(remaining)}`;
    if (myTeam) startStreaming();
  } else if (battle.status === "ended") {
    statusEl.textContent = "battle ended";
    banner.classList.add("show");
    const o = battle.outcome;
    if (!o) {
      banner.textContent = "battle ended";
    } else if (o.kind === "tie") {
      banner.textContent = "battle ended in a tie";
    } else {
      const label = o.winner === "court_square" ? "Team Court Square" : "Team Church Ave";
      const via = o.kind === "elimination" ? "by eliminating the opposing team" : "on time, by percentage";
      banner.textContent = `${label} wins ${via}!`;
    }
  }

  renderMyStatus();
}

ws.onmessage = (event) => {
  latest = JSON.parse(event.data);
  render();
};

// keep the countdown ticking smoothly between server pushes
setInterval(() => { if (latest && latest.battle.status === "active") render(); }, 1000);
</script>
</body>
</html>"#;
