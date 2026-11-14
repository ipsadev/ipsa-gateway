use anyhow::{anyhow, Result};
use ibc::{
    clients::tendermint::consensus_state::ConsensusState as TmConsensusState,
    primitives::proto::{Any, Protobuf},
};
use ibc_proto::ibc::lightclients::tendermint::v1::ConsensusState as RawTmConsensusState;

pub const TENDERMINT_CONSENSUS_STATE_TYPE_URL: &str =
    "/ibc.lightclients.tendermint.v1.ConsensusState";

#[derive(Clone, Debug)]
pub enum AnyConsensusState {
    Tendermint(TmConsensusState),
}

impl Protobuf<Any> for AnyConsensusState {}

impl TryFrom<Any> for AnyConsensusState {
    type Error = anyhow::Error;

    fn try_from(raw: Any) -> Result<Self> {
        match raw.type_url.as_str() {
            TENDERMINT_CONSENSUS_STATE_TYPE_URL => Ok(Self::Tendermint(
                Protobuf::<RawTmConsensusState>::decode_vec(raw.value.as_slice())
                    .map_err(|e| anyhow!("decode tendermint consensus state: {e}"))?,
            )),
            other => Err(anyhow!("unknown consensus state type_url: {other}")),
        }
    }
}

impl From<AnyConsensusState> for Any {
    fn from(value: AnyConsensusState) -> Self {
        match value {
            AnyConsensusState::Tendermint(cons) => Self {
                type_url: TENDERMINT_CONSENSUS_STATE_TYPE_URL.to_string(),
                value: Protobuf::<RawTmConsensusState>::encode_vec(cons),
            },
        }
    }
}

impl From<TmConsensusState> for AnyConsensusState {
    fn from(cons: TmConsensusState) -> Self {
        Self::Tendermint(cons)
    }
}

impl AnyConsensusState {
    pub fn decode_value(bytes: &[u8]) -> Result<Self> {
        let cons = Protobuf::<RawTmConsensusState>::decode_vec(bytes)
            .map_err(|e| anyhow!("decode tendermint consensus state value: {e}"))?;
        Ok(Self::Tendermint(cons))
    }

    pub fn encode_value(&self) -> Vec<u8> {
        match self {
            Self::Tendermint(cons) => Protobuf::<RawTmConsensusState>::encode_vec(cons.clone()),
        }
    }

    pub fn timestamp_secs(&self) -> u64 {
        match self {
            Self::Tendermint(cons) => cons.inner().timestamp.unix_timestamp() as u64,
        }
    }
}
