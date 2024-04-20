Hashsync

# Usage

```rust
let mut hs = HashSync::new();
hs.insert((1, 2));
hs.insert((1, 3));
hs.insert((3, 4));
let index = hs.index(|&(a, _b)| a);

hs.insert((1, 4));
let rows = index.get_values(&1);
assert_eq!(rows.len(), 3);
assert!(rows.contains(&(1, 2)));
assert!(rows.contains(&(1, 3)));
assert!(rows.contains(&(1, 4)));
```

Time complexity:
- Index lookups are amortized `O(1)` (backed by a `HashMap`).
- Adding new indexes is `O(n)` where `n` is the current number of rows.
- Insertions are amortized `O(n)` where `n` is the number of indexes.

# Future optimizations
- Reduce copying
- Drop indexes that are no longer in use
