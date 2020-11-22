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

### Further improvements
Use a BTreeMap representing a window of disputable transactions in the `State::transactions` struct. Current approach 
keeps a map of all disputable transactions 
