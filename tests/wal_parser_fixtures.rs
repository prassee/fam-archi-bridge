use bytes::Bytes;
use pg_walstream::protocol::{build_begin_message, build_commit_message, build_relation_message, build_insert_message, build_update_message, build_delete_message};
use rust_wal_cake_writer::wal_parser::WalParser;

#[test]
fn parse_insert_update_delete_roundtrip() {
    // Build a small relation + insert + update + delete sequence using pg_walstream helpers
    let rel = build_relation_message(1, "public", "t", 1, vec![(1u8, "id", 23u32, -1i32), (0u8, "name", 25u32, -1i32)]).unwrap();

    let insert = build_insert_message(1, vec![Some("1".as_bytes().to_vec()), Some("alice".as_bytes().to_vec())]).unwrap();

    let update = build_update_message(1, None, vec![Some("1".as_bytes().to_vec()), Some("alice2".as_bytes().to_vec())]).unwrap();

    let delete = build_delete_message(1, vec![Some("1".as_bytes().to_vec()), None]).unwrap();

    let mut payload = Vec::new();
    payload.extend_from_slice(&rel);
    payload.extend_from_slice(&insert);
    payload.extend_from_slice(&update);
    payload.extend_from_slice(&delete);

    let p = WalParser::new();
    let records = p.parse(&Bytes::from(payload)).unwrap();
    // We expect at least 3 records (insert, update, delete)
    assert!(records.len() >= 3);
}
