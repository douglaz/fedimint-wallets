//! Golden tests for [`MoveMeta`] — the `custom_meta` JSON the executor commits into every
//! lnv2 receive/send operation (spec §4/§5) and that `backfill_ops` decodes back. The shape is
//! load-bearing: it is how a lost `MoveRecord` is repaired from the op-log, so the on-the-wire
//! field names + the `DirectInflow` `from`-omission must be pinned.

use wallet_core::{FederationId, IdempotencyKey, Msat};
use wallet_fedimint::{move_protocol::RECEIVE_CONTRACT_QUOTED_META_KEY, MoveMeta, MoveRole};

const FED_A: FederationId = FederationId([0xAA; 32]);
const FED_B: FederationId = FederationId([0xBB; 32]);

fn key(s: &str) -> IdempotencyKey {
    IdempotencyKey(s.to_string())
}

#[test]
fn direct_inflow_receive_meta_omits_from_and_round_trips() {
    // Exactly the meta the executor builds at `CreateInvoice` for a DirectInflow (receive-only,
    // `from = None`).
    let meta = MoveMeta {
        move_id: key("direct-inflow:bb..bb:100000:1100000:0"),
        role: MoveRole::Receive,
        amount: Msat(100_000),
        from: None,
        to: FED_B,
    };
    let value = meta.to_value();

    // `move_id` is the join key (a plain string), `role` is lowercase, `to` is the 32-byte id as
    // a JSON array, and `from` is OMITTED entirely (skip_serializing_if on the `None`).
    let expected = serde_json::json!({
        "move_id": "direct-inflow:bb..bb:100000:1100000:0",
        "role": "receive",
        "amount": 100_000,
        "to": vec![0xBBu8; 32],
    });
    assert_eq!(value, expected);
    assert!(
        value.get("from").is_none(),
        "a receive-only DirectInflow meta must not carry a `from` field"
    );

    // Backfill decodes it straight back (the `move_id` is present, so it is recognised as a move
    // op, and the absent `from` defaults to `None`).
    assert_eq!(MoveMeta::from_value(&value), Some(meta));
}

#[test]
fn move_send_meta_carries_from_and_round_trips() {
    // A `Move` send leg tags both endpoints; `from` is present.
    let meta = MoveMeta {
        move_id: key("move:aa..aa:bb..bb:0"),
        role: MoveRole::Send,
        amount: Msat(100_000),
        from: Some(FED_A),
        to: FED_B,
    };
    let value = meta.to_value();

    assert_eq!(value.get("role").and_then(|r| r.as_str()), Some("send"));
    assert_eq!(
        value.get("from").cloned(),
        Some(serde_json::json!(vec![0xAAu8; 32])),
        "a Move send meta must carry its source federation"
    );
    assert_eq!(MoveMeta::from_value(&value), Some(meta));
}

#[test]
fn receive_meta_carries_replayable_contract_quote_without_breaking_backfill_decode() {
    let meta = MoveMeta {
        move_id: key("move:receive-contract-quote"),
        role: MoveRole::Receive,
        amount: Msat(100_000),
        from: Some(FED_A),
        to: FED_B,
    };
    let value = meta.receive_value_with_contract_quote(Msat(100_450));

    assert_eq!(
        value.get(RECEIVE_CONTRACT_QUOTED_META_KEY).cloned(),
        Some(serde_json::json!(100_450)),
        "the receive op must durably carry the quoted contract for crash-resume verification"
    );
    assert_eq!(
        MoveMeta::receive_contract_quote_from_value(&value).expect("valid quote field"),
        Some(Msat(100_450))
    );
    assert_eq!(
        MoveMeta::from_value(&value),
        Some(meta),
        "extra receive-only metadata must not break ordinary MoveMeta backfill"
    );

    let missing = serde_json::json!({ "move_id": "x", "role": "receive" });
    assert_eq!(
        MoveMeta::receive_contract_quote_from_value(&missing).expect("missing is not corrupt"),
        None
    );

    let mut malformed = serde_json::Map::new();
    malformed.insert(
        RECEIVE_CONTRACT_QUOTED_META_KEY.to_string(),
        serde_json::json!("not-a-msat"),
    );
    let malformed = serde_json::Value::Object(malformed);
    assert!(
        MoveMeta::receive_contract_quote_from_value(&malformed).is_err(),
        "a present but undecodable quote is corrupt metadata"
    );
}

#[test]
fn non_move_custom_meta_is_not_a_move_meta() {
    // A bare `wallet-cli receive`/`pay` tags only a `role`, never a `move_id`; decoding it as a
    // MoveMeta fails (backfill treats a missing `move_id` as "not part of a move" and skips it).
    let bare = serde_json::json!({ "role": "receive" });
    assert_eq!(MoveMeta::from_value(&bare), None);
}
