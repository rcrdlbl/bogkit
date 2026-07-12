//! Pure "scan for nearby opponents" decision logic.
//!
//! Deliberately decoupled from fold, same discipline as `crate::win`:
//! `battle.rs` resolves a player's own last fix and every live opponent's
//! last fix out of the presence stream, then hands plain values in here —
//! nothing below touches a `Stream`, a store, or any I/O, so it's
//! unit-testable on its own without a live pipeline.

use serde::Serialize;

/// A player must wait this long between uses of the nearby-opponent scan.
pub const SCAN_COOLDOWN_MS: u64 = 5 * 60 * 1000;
/// "Immediate vicinity" for the scan: 20 feet.
pub const NEARBY_RADIUS_METERS: f64 = 6.096;

/// Degrees-of-latitude to meters is constant; degrees-of-longitude to
/// meters shrinks by `cos(latitude)` — the standard equirectangular
/// approximation. Accurate to well under a meter at the ~6m scale this tool
/// cares about, far simpler than a full haversine, and there's no existing
/// distance helper anywhere in the workspace to reuse (only axis-aligned
/// bbox containment, see `domain::Bbox`).
const METERS_PER_DEGREE_LAT: f64 = 111_320.0;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    pub lat: f64,
    pub lon: f64,
}

pub fn meters_between(a: GeoPoint, b: GeoPoint) -> f64 {
    let lat_rad = a.lat.to_radians();
    let dy = (b.lat - a.lat) * METERS_PER_DEGREE_LAT;
    let dx = (b.lon - a.lon) * METERS_PER_DEGREE_LAT * lat_rad.cos();
    dx.hypot(dy)
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScanOutcome {
    NearbyCount { count: u32 },
    OnCooldown { remaining_ms: u64 },
    /// Not in an active battle, not on a roster, or no recent location fix
    /// to scan from yet — all collapse to one generic "can't scan right now"
    /// reply; none of them are the player's fault in a way worth
    /// distinguishing, and none consume the cooldown.
    Unavailable,
}

/// `my_fix: None` covers every disqualifying case at once (see
/// [`ScanOutcome::Unavailable`]'s doc) — the caller resolves eligibility down
/// to a single `Option` before calling this. The caller should only update
/// its cooldown-tracking map when this returns `NearbyCount`.
pub fn decide_scan(
    now_ms: u64,
    last_scan_ms: Option<u64>,
    my_fix: Option<GeoPoint>,
    opponent_fixes: &[GeoPoint],
) -> ScanOutcome {
    if let Some(last) = last_scan_ms {
        let elapsed = now_ms.saturating_sub(last);
        if elapsed < SCAN_COOLDOWN_MS {
            return ScanOutcome::OnCooldown { remaining_ms: SCAN_COOLDOWN_MS - elapsed };
        }
    }
    let Some(me) = my_fix else {
        return ScanOutcome::Unavailable;
    };
    let count = opponent_fixes.iter().filter(|&&p| meters_between(me, p) <= NEARBY_RADIUS_METERS).count();
    ScanOutcome::NearbyCount { count: count as u32 }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pt(lat: f64, lon: f64) -> GeoPoint {
        GeoPoint { lat, lon }
    }

    #[test]
    fn same_point_is_zero_distance() {
        let a = pt(40.7128, -74.0060);
        assert_eq!(meters_between(a, a), 0.0);
    }

    #[test]
    fn one_degree_of_latitude_is_about_111km() {
        let a = pt(40.0, -74.0);
        let b = pt(41.0, -74.0);
        let d = meters_between(a, b);
        assert!((d - 111_320.0).abs() < 50.0, "got {d}");
    }

    #[test]
    fn longitude_degrees_shrink_with_latitude() {
        // Same delta-longitude, but near the pole the great-circle distance
        // should be much smaller than at the equator.
        let equator = meters_between(pt(0.0, 0.0), pt(0.0, 1.0));
        let near_pole = meters_between(pt(80.0, 0.0), pt(80.0, 1.0));
        assert!(near_pole < equator / 3.0, "equator={equator} near_pole={near_pole}");
    }

    #[test]
    fn no_prior_scan_and_a_fix_counts_opponents_in_radius() {
        let me = pt(40.0, -74.0);
        // ~3m east: well within 20ft/6.096m.
        let near = pt(40.0, -74.0 + 3.0 / (METERS_PER_DEGREE_LAT * 40f64.to_radians().cos()));
        // ~50m east: outside the radius.
        let far = pt(40.0, -74.0 + 50.0 / (METERS_PER_DEGREE_LAT * 40f64.to_radians().cos()));
        let outcome = decide_scan(1_000_000, None, Some(me), &[near, far]);
        assert_eq!(outcome, ScanOutcome::NearbyCount { count: 1 });
    }

    #[test]
    fn no_fix_is_unavailable_regardless_of_cooldown() {
        let outcome = decide_scan(1_000_000, None, None, &[]);
        assert_eq!(outcome, ScanOutcome::Unavailable);
    }

    #[test]
    fn still_on_cooldown_reports_remaining_time_and_ignores_fixes() {
        let me = pt(40.0, -74.0);
        let outcome = decide_scan(SCAN_COOLDOWN_MS / 2, Some(0), Some(me), &[me]);
        assert_eq!(outcome, ScanOutcome::OnCooldown { remaining_ms: SCAN_COOLDOWN_MS / 2 });
    }

    #[test]
    fn cooldown_elapsed_allows_a_fresh_scan() {
        let me = pt(40.0, -74.0);
        let outcome = decide_scan(SCAN_COOLDOWN_MS, Some(0), Some(me), &[me]);
        assert_eq!(outcome, ScanOutcome::NearbyCount { count: 1 });
    }

    #[test]
    fn exactly_at_the_radius_boundary_counts_as_nearby() {
        let me = pt(0.0, 0.0);
        let edge = pt(0.0, NEARBY_RADIUS_METERS / METERS_PER_DEGREE_LAT);
        let outcome = decide_scan(0, None, Some(me), &[edge]);
        assert_eq!(outcome, ScanOutcome::NearbyCount { count: 1 });
    }
}
