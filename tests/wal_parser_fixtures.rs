use bytes::Bytes;
use rust_wal_cake_writer::wal_parser::WalParser;

#[test]
fn parse_insert_update_delete_roundtrip() {
    // Build relation message similar to protocol tests
    let mut rel = Vec::new();
    rel.push(b'R');
    rel.extend_from_slice(&1u32.to_be_bytes());
    rel.extend_from_slice(b"public\0");
    rel.extend_from_slice(b"t\0");
    rel.push(b'd'); // replica identity
    rel.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
    // col1
    rel.push(1u8); // flags
    rel.extend_from_slice(b"id\0");
    rel.extend_from_slice(&23u32.to_be_bytes());
    rel.extend_from_slice(&(-1i32).to_be_bytes());
    // col2
    rel.push(0u8);
    rel.extend_from_slice(b"name\0");
    rel.extend_from_slice(&25u32.to_be_bytes());
    rel.extend_from_slice(&(-1i32).to_be_bytes());

    // Build insert: I + relation_id + 'N' + tuple
    let mut ins = Vec::new();
    ins.push(b'I');
    ins.extend_from_slice(&1u32.to_be_bytes());
    ins.push(b'N');
    ins.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
    // id text '1'
    ins.push(b't');
    ins.extend_from_slice(&1u32.to_be_bytes());
    ins.extend_from_slice(b"1");
    // name text 'alice'
    ins.push(b't');
    ins.extend_from_slice(&5u32.to_be_bytes());
    ins.extend_from_slice(b"alice");

    // Build update: U + relation_id + new tuple (no old)
    let mut upd = Vec::new();
    upd.push(b'U');
    upd.extend_from_slice(&1u32.to_be_bytes());
    // No old tuple
    upd.push(b'N');
    upd.extend_from_slice(&2u16.to_be_bytes());
    upd.push(b't');
    upd.extend_from_slice(&1u32.to_be_bytes());
    upd.extend_from_slice(b"1");
    upd.push(b't');
    upd.extend_from_slice(&6u32.to_be_bytes());
    upd.extend_from_slice(b"alice2");

    // Build delete: D + relation_id + key_type + tuple
    let mut del = Vec::new();
    del.push(b'D');
    del.extend_from_slice(&1u32.to_be_bytes());
    del.push(b'K');
    del.extend_from_slice(&2u16.to_be_bytes());
    del.push(b't');
    del.extend_from_slice(&1u32.to_be_bytes());
    del.extend_from_slice(b"1");
    // second column null
    del.push(b'n');

    let mut payload = Vec::new();
    payload.extend_from_slice(&rel);
    payload.extend_from_slice(&ins);
    payload.extend_from_slice(&upd);
    payload.extend_from_slice(&del);

    let p = WalParser::new();
    println!("payload len={}", payload.len());
    let records = p.parse(&Bytes::from(payload)).unwrap();
    // We expect at least 3 records (insert, update, delete)
    assert!(records.len() >= 3, "records={}", records.len());
}
