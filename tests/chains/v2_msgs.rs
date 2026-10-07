use ipsa_chains::{
    msg::{ProtoAny, ProtoMessage},
    v2_msgs::{Height, MsgRecvPacket, Packet, Payload, TYPE_URL_RECV_PACKET},
};

#[test]
fn recv_packet_encodes_into_any() {
    let msg = MsgRecvPacket {
        packet: Some(Packet {
            sequence: 1,
            source_client: "07-tendermint-5".to_string(),
            dest_client: "08-wasm-0".to_string(),
            timeout_timestamp: 1_784_946_743,
            payloads: vec![Payload {
                source_port: "transfer".to_string(),
                dest_port: "transfer".to_string(),
                version: "ics20-1".to_string(),
                encoding: "application/json".to_string(),
                value: b"{}".to_vec(),
            }],
        }),
        proof_commitment: b"mock".to_vec(),
        proof_height: Some(Height {
            revision_number: 0,
            revision_height: 3_785_598,
        }),
        signer: "cosmos1mz5kk8dh85ss9dlx5e99cvkl60x92fkmtk6rxj".to_string(),
    };
    let any: ProtoAny = msg.clone().into();

    assert_eq!(any.type_url, TYPE_URL_RECV_PACKET);
    assert_eq!(MsgRecvPacket::decode(any.value.as_slice()).unwrap(), msg);
}
