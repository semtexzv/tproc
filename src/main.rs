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

// Use f32 for value for now, later replace with decimal representation for
// avoiding errors
pub type Value = f64;

pub struct Account {
    available: Value,
    held: Value,
    locked: bool
}

pub struct State {
    accounts: HashMap<u16, Account>,
}
pub fn process_stream(r : impl Read) -> Result<()> {

}


fn main() {
    let args: Vec<_> = args().collect();
    ensure!(args.len() > 1, "Missing input file argument");
    let ifile = std::fs::File::open(&args[1])?;

    println!("Hello, world!");
}
