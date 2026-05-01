use bytes::Bytes;
use rust_wal_cake_writer::wal_parser::{WalParser};

#[test]
fn parse_empty_returns_empty() {
    let p = WalParser::new();
    let res = p.parse(&Bytes::new()).unwrap();
    assert!(res.is_empty());
}

// Additional tests for Insert/Update/Delete require constructing pgoutput
// payloads. For now ensure basic integration compiles. More comprehensive
// fixtures can be added later using pg_walstream test helpers.
