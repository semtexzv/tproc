## Transcation Processor
Takes csv [input](./data/0.in.csv), interprets it as a stream of transactions, and applies
them to an intermediate state, outputting it to stdout in the end. 

### Running
```
cargo run -- ./data/0.in.csv
```
Or:
```
./test.sh
```
### Libraries used
[Serde](https://docs.rs/serde/1.0.117/serde/) + [Csv](https://docs.rs/csv/1.1.4/csv/) for obvious reasons. 

[Anyhow](https://docs.rs/anyhow/1.0.34/anyhow/) - Perfect for simple error handling in these small applications.

[rust_decimal](https://docs.rs/rust_decimal/1.8.1/rust_decimal/) - We are handling monetary amounts, rounding errors are unnaceptable. 
Rather than writing own decimal type, I reached for finished library which handles all corner cases

### Further improvements
Use a BTreeMap representing a window of disputable transactions in the `State::transactions` struct. Current approach 
keeps a map of all disputable transactions, thus the memory can grow indefinitely.
