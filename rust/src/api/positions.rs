use anyhow::Result;
use reqwest::Client;
use crate::config::API;
use crate::types::{Direction, ServerPositionState};

pub async fn search_open(
    client: &Client,
    bearer: &str,
    account_id: i64,
    contract_id: &str,
) -> Result<ServerPositionState> {
    let resp: serde_json::Value = client
        .post(format!("{}/Position/searchOpen", API))
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "accountId": account_id }))
        .send()
        .await?
        .json()
        .await?;

    let positions = resp["positions"].as_array().cloned().unwrap_or_default();

    if positions.is_empty() {
        return Ok(ServerPositionState::Flat);
    }

    let same = positions
        .iter()
        .find(|p| p["contractId"].as_str().unwrap_or("") == contract_id);

    match same {
        None => Ok(ServerPositionState::Flat),
        Some(p) => {
            let size = extract_size(p);
            let avg_price = extract_avg_price(p);
            let side = extract_side(p);
            Ok(ServerPositionState::Open { size, avg_price, side })
        }
    }
}

fn extract_size(p: &serde_json::Value) -> u32 {
    for key in &["size", "qty", "quantity"] {
        if let Some(v) = p[key].as_f64() {
            return v.abs() as u32;
        }
    }
    0
}

fn extract_avg_price(p: &serde_json::Value) -> Option<f64> {
    for key in &["averagePrice", "avgPrice", "avgFillPrice", "entryPrice", "openPrice"] {
        if let Some(v) = p[key].as_f64() {
            if v > 0.0 {
                return Some(v);
            }
        }
    }
    None
}

fn extract_side(p: &serde_json::Value) -> Option<serde_json::Value> {
    for key in &["side", "direction", "positionSide", "type"] {
        if !p[key].is_null() {
            return Some(p[key].clone());
        }
    }
    None
}

#[allow(dead_code)]
pub async fn close_contract(
    client: &Client,
    bearer: &str,
    account_id: i64,
    contract_id: &str,
) -> Result<bool> {
    let resp: serde_json::Value = client
        .post(format!("{}/Position/closeContract", crate::config::API))
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "accountId": account_id, "contractId": contract_id }))
        .send()
        .await?
        .json()
        .await?;
    Ok(resp["success"].as_bool().unwrap_or(false))
}

#[allow(dead_code)]
pub async fn partial_close_contract(
    client: &Client,
    bearer: &str,
    account_id: i64,
    contract_id: &str,
    size: u32,
) -> Result<bool> {
    let resp: serde_json::Value = client
        .post(format!("{}/Position/partialCloseContract", crate::config::API))
        .header("Authorization", bearer)
        .json(&serde_json::json!({
            "accountId": account_id,
            "contractId": contract_id,
            "size": size,
        }))
        .send()
        .await?
        .json()
        .await?;
    Ok(resp["success"].as_bool().unwrap_or(false))
}

/// Map a server-side position side/type value to a position direction.
///
/// Broker convention (Position/searchOpen "type" field, verified Nov 18, 2026
/// against a raw dump of a known long position): **1 = Long, 2 = Short**.
/// These are POSITION-type values — do not confuse them with trade-side
/// values (0 = buy, 1 = sell). The old code assumed 0 = Long / 1 = Short,
/// which made every real long (type 1) read as a short and produced the
/// wrong-way BUY "close" orders of Nov 18, 2026.
///
/// Any other value returns None: callers must NOT guess a direction.
pub fn direction_from_side(side: &serde_json::Value) -> Option<Direction> {
    if let Some(n) = side.as_i64() {
        return match n {
            1 => Some(Direction::Long),
            2 => Some(Direction::Short),
            _ => None,
        };
    }
    if let Some(s) = side.as_str() {
        return match s.to_uppercase().as_str() {
            "LONG" | "BUY" | "B" => Some(Direction::Long),
            "SHORT" | "SELL" | "S" => Some(Direction::Short),
            _ => None,
        };
    }
    None
}

/// Returns the close action ("BUY" or "SELL") from a server-side position
/// side value: a Long closes with SELL, a Short closes with BUY.
/// Returns None for any unrecognized value — never guess.
pub fn close_action_from_side(side: &serde_json::Value) -> Option<&'static str> {
    match direction_from_side(side) {
        Some(Direction::Long) => Some("SELL"),
        Some(Direction::Short) => Some("BUY"),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn long_type_1_closes_with_sell() {
        assert_eq!(direction_from_side(&json!(1)), Some(Direction::Long));
        assert_eq!(close_action_from_side(&json!(1)), Some("SELL"));
    }

    #[test]
    fn short_type_2_closes_with_buy() {
        assert_eq!(direction_from_side(&json!(2)), Some(Direction::Short));
        assert_eq!(close_action_from_side(&json!(2)), Some("BUY"));
    }

    #[test]
    fn zero_is_unknown_not_long() {
        // Regression test for the Nov 18, 2026 bug: the old mapping treated
        // 0 as Long and every other value (including a real long, type 1)
        // as Short. 0 is a trade-side value (buy), not a position type.
        assert_eq!(direction_from_side(&json!(0)), None);
        assert_eq!(close_action_from_side(&json!(0)), None);
    }

    #[test]
    fn other_numbers_are_unknown() {
        for v in [json!(3), json!(-1), json!(100)] {
            assert_eq!(direction_from_side(&v), None);
            assert_eq!(close_action_from_side(&v), None);
        }
    }

    #[test]
    fn string_forms() {
        assert_eq!(close_action_from_side(&json!("LONG")), Some("SELL"));
        assert_eq!(close_action_from_side(&json!("SHORT")), Some("BUY"));
        assert_eq!(close_action_from_side(&json!("long")), Some("SELL"));
        assert_eq!(close_action_from_side(&json!("short")), Some("BUY"));
        assert_eq!(direction_from_side(&json!("LONG")), Some(Direction::Long));
        assert_eq!(direction_from_side(&json!("SHORT")), Some(Direction::Short));
    }

    #[test]
    fn garbage_and_null_are_unknown() {
        assert_eq!(direction_from_side(&json!(null)), None);
        assert_eq!(close_action_from_side(&json!(null)), None);
        assert_eq!(direction_from_side(&json!("SIDEWAYS")), None);
        assert_eq!(close_action_from_side(&json!("SIDEWAYS")), None);
        assert_eq!(direction_from_side(&json!(1.5)), None);
    }
}
