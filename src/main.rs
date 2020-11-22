use serde::{Serialize, Deserialize};
use std::env::args;

use anyhow::*;
use std::collections::HashMap;
use std::io::{Read, Write};
use rust_decimal::prelude::Zero;

// Use serde to parse entries,
// Apply to mutable state
// Keep only withdrawals + deposits for chargebacks etcs
// Preserve transaction state for preventing double chargebacks
// Allow accounts to go to negative value in order to allow multiple disputes

// Use f32 for value for now, later replace with decimal representation
// in order to avoid rounding errors
pub type Value = rust_decimal::Decimal;

#[derive(Debug, Deserialize, PartialOrd, PartialEq)]
pub enum EntryType {
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

impl EntryType {
    pub fn is_tx(&self) -> bool {
        self == &EntryType::Deposit || self == &EntryType::Withdrawal
    }
    pub fn is_op(&self) -> bool {
        self != &EntryType::Deposit && self != &EntryType::Withdrawal
    }
}

#[derive(Debug, PartialOrd, PartialEq)]
pub enum EntryState {
    New,
    Processed,
    Failed,
    Disputed,
    Resolved,
    Chargeback,
}

impl Default for EntryState {
    fn default() -> Self {
        EntryState::New
    }
}

// Would've used a internally tagged enum, but serde csv doesn't like it
// https://serde.rs/enum-representations.html
/// Entry is an element of input. It is either mutable transaction or an operation which manipulates
/// another mutable transaction. For this purposes we represent them in a single struct
#[derive(Debug, Deserialize)]
pub struct Entry {
    #[serde(rename = "type")]
    typ: EntryType,
    client: u16,
    #[serde(rename = "tx")]
    id: u32,
    amount: Option<Value>,

    #[serde(skip_serializing, skip_deserializing)]
    state: EntryState,
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
    transactions: HashMap<u32, Entry>,
}

impl State {
    /// Apply an entry from the input
    pub fn apply(&mut self, tx: Entry) -> Result<()> {
        if tx.typ.is_tx() {
            self.apply_tx(tx)
        } else {
            self.apply_op(tx)
        }
    }
    // Apply a transaction to an account if possible, if not possible, which provides more information
    pub fn apply_tx(&mut self, mut tx: Entry) -> Result<()> {
        let acc = self.accounts
            .entry(tx.client)
            .or_insert_with(|| Default::default());

        let amount = tx.amount.ok_or(Error::msg("Expected amount associated"))?;

        match &tx.typ {
            EntryType::Deposit => {
                acc.available += amount;

                tx.state = EntryState::Processed;
                self.transactions.insert(tx.id, tx);
            }
            EntryType::Withdrawal => {
                let res = acc.available - amount;
                if res < Value::zero() {
                    tx.state = EntryState::Failed;
                    bail!("Invalid withdrawal, not enough funds");
                } else {
                    acc.available -= amount;
                    tx.state = EntryState::Processed
                }
                self.transactions.insert(tx.id, tx);
            }
            _ => bail!("Invalid transaction: {:?}", tx)
        }
        Ok(())
    }

    // Apply an operation to pre-existing transaction
    pub fn apply_op(&mut self, op: Entry) -> Result<()> {
        let actual = self.transactions.get_mut(&op.id);
        let actual = actual.ok_or_else(|| Error::msg("Tx not found"))?;
        ensure!(actual.client == op.client, "Operation referencing tx of a different client");

        let acc = self.accounts.get_mut(&op.client)
            .ok_or_else(|| Error::msg("Client missing"))?;
        let amount = actual.amount.ok_or_else(|| Error::msg("Missing amount"))?;

        match &op.typ {
            // Allowing disputes of both deposits and withdrawals for now, spec requires us to lock funds
            // This seems weird from my position, but let's follow the spec and see from there
            EntryType::Dispute => {
                ensure!(actual.typ.is_tx(), "Attempting to dispute {:?}", actual.typ);
                // Dispute -> resolve -> dispute flow sounds possible, let's allow it
                ensure!(actual.state == EntryState::Processed || actual.state == EntryState::Resolved,
                    "Attempting to dispute tx in state: {:?}", actual.state);

                actual.state = EntryState::Disputed;
                acc.held += amount;
                acc.available -= amount;
            }
            EntryType::Resolve => {
                ensure!(acc.held >= amount, "Client held funds missing");
                ensure!(actual.state == EntryState::Disputed, "Only disputed transactions can be resolved");

                actual.state = EntryState::Resolved;
                acc.held -= amount;
                acc.available += amount;
            }
            EntryType::Chargeback => {
                ensure!(acc.held >= amount, "Client held funds missing");
                ensure!(actual.state == EntryState::Disputed, "Only disputed transactions can be charged back");

                actual.state = EntryState::Chargeback;
                acc.held -= amount;
                acc.locked = true;
            }
            _ => bail!("Invalid operation {:?}", op),
        }
        Ok(())
    }


    pub fn write(&self, w: impl Write) -> Result<()> {
        // Different fields than our inner account repr, create local temp struct for output
        #[derive(Serialize)]
        struct AccountOut {
            client: u16,
            available: Value,
            held: Value,
            total: Value,
            locked: bool,
        }
        let mut writer = csv::Writer::from_writer(w);
        for (id, acc) in &self.accounts {
            writer.serialize(&AccountOut {
                client: *id,
                available: acc.available,
                held: acc.held,
                total: acc.available + acc.held,
                locked: acc.locked,
            })?;
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
        .comment(Some(b'#'))
        .terminator(csv::Terminator::CRLF)
        .from_reader(r);

    for tx in rdr.deserialize() {
        let tx: Entry = tx?;
        if let Err(e) = state.apply(tx) {
            eprintln!("Error: {}", e);
        }
    }

    Ok(state)
}


fn main() -> Result<()> {
    let args: Vec<_> = args().collect();
    ensure!(args.len() > 1, "Missing input file argument");
    let ifile = std::fs::File::open(&args[1])?;
    let state = process_stream(ifile)?;
    state.write(std::io::stdout())?;
    Ok(())
}
