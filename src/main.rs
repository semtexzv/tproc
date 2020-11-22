use serde::{Serialize, Deserialize};
use std::env::args;

use anyhow::*;
use std::collections::HashMap;
use std::io::Read;

// Use serde to parse entries,
// Apply to mutable state
// Keep only withdrawals + deposits for chargebacks etcs
// Preserve transaction state for preventing double chargebacks
// Allow accounts to go to negative value in order to allow multiple disputes

// Use f32 for value for now, later replace with decimal representation
// in order to avoid rounding errors
pub type Value = f64;

#[derive(Debug, Deserialize, PartialOrd, PartialEq)]
pub enum TxType {
    #[serde(rename = "deposit")]
    Deposit,
    #[serde(rename = "withdrawal")]
    Withdrawal,
    #[serde(rename = "dispute")]
    Dispute,
    #[serde(rename = "resolve")]
    Resolve,
    #[serde(rename = "chargeback")]
    Chargeback,
}

#[derive(Debug, Deserialize)]
pub struct Tx {
    #[serde(rename = "type")]
    t: TxType,
    client: u16,
    #[serde(rename = "tx")]
    id: u32,
    amount: Option<Value>,
}

#[derive(Debug, Default)]
pub struct Account {
    available: Value,
    held: Value,
    locked: bool,
}

#[derive(Debug, Default)]
pub struct State {
    accounts: HashMap<u16, Account>,
    // Replace with BTreeMap, and remove old transactions in order to keep memory low
    // (limited dispute window)
    transactions: HashMap<u32, Tx>,
}

impl State {
    pub fn apply(&mut self, tx: Tx) -> Result<()> {
        match tx.t {
            TxType::Deposit => {}
            TxType::Withdrawal => {}
            TxType::Dispute => {}
            TxType::Resolve => {}
            TxType::Chargeback => {}
        }
        Ok(())
    }
}

pub fn process_stream(r: impl Read) -> Result<State> {
    let mut state = State::default();

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(csv::Trim::All)
        .terminator(csv::Terminator::CRLF)
        .from_reader(r);

    for tx in rdr.deserialize() {
        let tx: Tx = tx?;
        state.apply(tx)
    }

    Ok(state)
}


fn main() -> Result<()> {
    let args: Vec<_> = args().collect();
    ensure!(args.len() > 1, "Missing input file argument");
    let ifile = std::fs::File::open(&args[1])?;
    let state = process_stream(ifile)?;

    println!("State: {:?}", state);
    Ok(())
}
