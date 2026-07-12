//! Area denial: a location-based team game where fold maintains two live
//! per-team aggregates — roster size and in-bounds presence — over a
//! stream of joins and GPS pings, broadcast to every browser over a
//! websocket. See `battle.rs` for the fold pipelines and win-condition
//! logic, `matches.rs` for the multi-match registry, `web.rs` for the
//! HTTP/websocket surface, `parks.rs` for the battleground catalog.
//!
//! Run with `cargo run -p mmo-cs`, then open http://localhost:3000 — host a
//! new match, pick a park and match length from the host link, share the
//! player link (or its QR code) so others can join a team in two browser
//! tabs (or two devices), start the battle once both teams have a member,
//! and use devtools' geolocation override (or a real phone) to move each
//! "player" in or out of the chosen park's bounding box. The homepage lists
//! every match currently running.

mod battle;
mod domain;
mod matches;
mod parks;
mod protocol;
mod web;
mod win;

fn main() {
    // MMO_DATA_DIR should point at a stable path for a real deployment: with
    // one, a restart resumes every match that hadn't ended yet (see
    // `matches::Registry::bootstrap`) instead of abandoning it, and a
    // tmp-cleaner or reboot can't wipe in-progress ones out from under it.
    // Defaults to a fresh temp dir for local dev, matching the other
    // example crates' convention — every local run starts with no matches.
    let data_dir = match std::env::var_os("MMO_DATA_DIR") {
        Some(dir) => std::path::PathBuf::from(dir),
        None => {
            let dir = std::env::temp_dir().join("bog-kit-mmo-cs");
            let _ = std::fs::remove_dir_all(&dir);
            dir
        }
    };

    let registry = matches::Registry::bootstrap(data_dir);
    web::serve(registry);
}
