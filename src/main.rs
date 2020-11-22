use serde::{Serialize, Deserialize};

// Use serde to parse entries,
// Apply to mutable state
// Keep only withdrawals + deposits for chargebacks etcs
// Preserve transaction state for preventing double chargebacks
// Allow accounts to go to negative value in order to allow multiple disputes

fn main() {
    println!("Hello, world!");
}
