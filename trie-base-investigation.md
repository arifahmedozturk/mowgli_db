# Trie base investigation (spike #59)

Should heavy-trie move from a binary trie (base 2) to base 4, 16 or 256?

## Summary

- **Point lookups:** wider bases cut the chains a lookup crosses: at 1M random keys, base 2 crosses 10.4, base 4 crosses 8.3, base 16 crosses 5.9 and base 256 crosses 3.9 (−44% at base 16). Inserts flip heavy paths 5× less often at base 16 and about 50× less at base 256.
- **Range scans:** no base helps. At every base each key is its own chain, so a scan over 1,000 consecutive keys touches about 1,000 chains. That one-chain-per-key layout is why range scans trail LMDB by 35–60×.
- **Space:** no base helps either. Chains average 30–47 B at every base, but each occupies a 256 B packed slot, so ~85% of the index is padding (#18).
- **Sweet spot: base 16.** It gets most of the lookup and flip benefit while nodes stay small (p99 chain < 200 B, max < 900 B). It needs a 16-bit child bitmap per node so finding a child stays O(1). Base 256 adds little over base 16 (nothing at all for decimal-string keys), and needs ART-style adaptive nodes and chains up to ~6 KB.
- **Recommendation: no-go for now.** Do #13 (subtree buckets) and #18 (slot size classes) first; they target the two largest gaps against the B-tree baselines, ranges and space, which a base change does not touch. Revisit base 16 afterwards if point-lookup latency is then the main gap. The two compose: base 16 above, buckets at the bottom.

## Why base 2 was chosen, and what changed

In a binary trie every branching node has exactly one heavy and one light child. Each chain node therefore carries one light pointer, nodes are small and uniform (13 B, or 23 B where a key can end), and a flip touches exactly one neighbouring chain. That kept the chains next to any node to a minimum.

Since then the costs have shifted. Lookups no longer decode chains (#15) and opening no longer walks the trie (#20, #50). What remains per lookup is mostly a fixed cost per chain crossed — pin, cache check, cache misses on the chain bytes — and comparing 64 bits one at a time is small next to that. This hasn't been profiled; it follows from #15 (removing per-chain decoding gave +30%) and #58 (a cache hit costing more than reading the mapped bytes). If it holds, fewer crossings matter more than a slightly larger node.

## Method

A throwaway simulator (not committed) builds the same structure as the engine — a path-compressed trie with heavy-path decomposition, heavy child = child with the most keys — at bases 2, 4, 16 and 256, over the `bench/keys.h` key sets at 1M keys. Keys are split into digits of 1, 2, 4 or 8 bits. For each base it measures:

- **Chains per lookup**: chains entered on the root-to-key walk (200K sampled keys).
- **Work per lookup**: digits compared, branch nodes passed, light-child entries scanned (linear scan over a node's sorted light children).
- **Chain size**: modelled by extending today's on-disk format: 18 B header, path digits, and per node with light children or a record 2 B split position + 2 B heavy weight + per light child (1 B digit if base > 2, 8 B pointer, 2 B weight) + 10 B record. At base 2 this gives 14 B per node vs 13 B today; the extra byte is a 2-byte split position (#32).
- **Range scans**: distinct chains touched by 100 scans of N/1000 consecutive keys.
- **Flips**: a separate simulation inserts 100K keys in random order into an uncompressed digit trie and counts how often a node's heavy child changes.
- **Buckets (#13)**: for base 2, chains crossed when every subtree of at most `cap` keys is stored as one sorted block (counted as one more block).

**Validation:** the base-2 numbers match the engine. Simulated chains per lookup are 8.75 at 100K and 10.42 at 1M random keys; `bench` measures 8.75 and 10.41.

## Results (1M keys)

### Chains crossed per lookup (mean / p99 / max)

| Key set | Base 2 | Base 4 | Base 16 | Base 256 |
|---|---|---|---|---|
| `random_u64` | 10.4 / 15 / 19 | 8.3 / 11 / 12 | **5.9** / 7 / 8 | 3.9 / 4 / 5 |
| `seq_u64` | 10.9 / 16 / 19 | 8.5 / 11 / 11 | **5.7** / 6 / 6 | 3.9 / 4 / 4 |
| `prefix_str` | 9.8 / 14 / 18 | 8.8 / 12 / 14 | **6.7** / 8 / 8 | 6.7 / 8 / 8 |
| `varlen_str` | 9.4 / 13 / 16 | 8.5 / 12 / 14 | **7.0** / 9 / 10 | 5.0 / 6 / 6 |

This follows the expected (1 − 1/k) · log_k(n) light edges: base k has log2(k)× fewer levels, but a larger share of steps go light. `prefix_str` keys are ASCII digits whose high nibble never varies, so base 256 behaves exactly like base 16.

### Work per lookup (`random_u64`)

| | Base 2 | Base 4 | Base 16 | Base 256 |
|---|---|---|---|---|
| Digits compared | 64 | 32 | 16 | 8 |
| Branch nodes passed | 20.3 | 10.7 | 5.7 | 3.1 |
| Light-child entries scanned (linear) | 9 | 14 | 34 | 262 |

At base 16 the child scan is fixed with a 16-bit bitmap per node: test the bit, then popcount to find the child's index, which is O(1). Base 256 needs adaptive node types (ART's 4/16/48/256) to avoid scanning hundreds of entries.

### Flips per insert (100K random-order inserts)

| Key set | Base 2 | Base 4 | Base 16 | Base 256 |
|---|---|---|---|---|
| `random_u64` | 0.50 | 0.30 | 0.11 | 0.010 |
| `seq_u64` | 0.59 | 0.24 | 0.04 | 0.001 |
| `prefix_str` | 0.56 | 0.37 | 0.09 | 0.09 |
| `varlen_str` | 0.42 | 0.32 | 0.18 | 0.06 |

A flip at base k may have up to k − 1 neighbouring light chains to re-home, but at base 16 there are 5× fewer of them.

### Chain size and space

| `random_u64` | Base 2 | Base 4 | Base 16 | Base 256 |
|---|---|---|---|---|
| Chains per key | 1.00 | 1.00 | 1.00 | 1.00 |
| Mean chain size | 38 B | 37 B | 36 B | 34 B |
| p99 / max chain size | 137 / 334 B | 155 / 426 B | 179 / 875 B | 230 / 5,967 B |
| Chains over the 256 B slot | 0.00% | 0.03% | 0.41% | 0.37% |
| Chains over an 8 KB block | 0 | 0 | 0 | 0 |

Every base gives exactly one chain per key: each key is the end of exactly one heavy path. Bytes per key therefore depend on how chains are stored, not on the base. Today each ~37 B chain occupies a 256 B slot — 27 slots fit a packed block, so ~303 B per key — which is the 304 B/key the benchmark measures for a bulk-loaded index. Slot size classes (#18) could bring that close to the chains' actual size (an estimate of ~40–60 B per key, at any base).

### Range scans

Chains touched per scan of 1,000 consecutive keys: 1,009 (base 2), 1,007 (base 4), 1,005 (base 16), 1,003 (base 256). One chain per key means one chain per scanned key, so no base changes range-scan cost. LMDB reads the same keys from a few adjacent leaf pages.

### Subtree buckets (#13) on base 2, for comparison

| Bucket capacity (keys) | 1 (today) | 16 | 64 | 128 | 256 |
|---|---|---|---|---|---|
| Chains + bucket per lookup, `random_u64` | 10.8 | 9.4 | 8.5 | 8.0 | 7.5 |
| Chains + bucket per lookup, `prefix_str` | 10.1 | 8.7 | 7.8 | 7.2 | 6.8 |

Buckets save fewer crossings than base 16 (7.5 vs 5.9 at 1M random keys). But a bucket stores up to `cap` adjacent keys in one sorted block, so a 1,000-key scan reads a handful of buckets instead of ~1,000 chains. They're the only option here that addresses range scans.

## Benefits and drawbacks

| | Base 4 | Base 16 | Base 256 |
|---|---|---|---|
| Chains per lookup at 1M | −20% | **−44%** | −62% (string keys: same as base 16) |
| Flips per insert | −40% | −80% | −98% |
| Node size | small | ≤ 15 light pointers (~170 B max) | up to 255 pointers; chains up to ~6 KB |
| Finding a child | trivial | 16-bit bitmap + popcount | adaptive node types (ART) |
| Range scans | no change | no change | no change |
| Space | no change | no change | no change |
| Where keys can end | every 2 bits → every byte | every nibble | every byte (natural) |

Base 4 isn't worth a format change for −20%. Base 256 gives the fewest crossings, but only for keys that use the whole byte range, and it costs complex nodes and large chains that no longer fit the 256 B slot design.

## Migration cost (rough)

The bit-level assumptions live in `index/` (about 1,900 lines including headers, mostly `disk_trie.cpp` and `chain.cpp`) plus the packed-slot code in `storage/disk_manager.cpp`, with about 125 places that index bits directly. A base-16 trie needs:

- **New on-disk chain format:** nibble path, nodes with a child bitmap and up to 15 light pointers, 2-byte split positions (also fixes #32). Existing files would need a rebuild; there's no reason to keep reading the old format.
- **Rewritten traversal and updates:** `chain_match` and `chain_match_raw`, insert (split, new-node, flip), remove, `bulk_insert`, the range cursor (in-order over up to 16 children per node) and both compaction layouts.
- **Weights and flips per child:** a flip swaps the heavy child with whichever light child outgrew it.
- **Tests:** rework of the chain, trie and compaction tests.

Estimate: 1–2 weeks of focused work, mostly in `disk_trie.cpp`, plus a benchmark round. Most of the cost is redoing the rebalancing and cursor logic for k children per node.

## Recommendation

**No-go for now; base 16 is the candidate if this is revisited.**

1. **Do #13 (subtree buckets) and #18 (slot size classes) first.** Against the B-tree baselines the largest gaps are range scans (35–60× behind LMDB) and space (4–6×), and the base doesn't affect either.
2. **Then re-measure point lookups against LMDB.** If lookups are still the main gap, switch to base 16 with a child bitmap. It would cut chain crossings by about 44% and flips by about 80%, and it combines with buckets: base-16 chains above, sorted buckets at the bottom.
3. **Skip base 4 and base 256.** Base 4 gains too little for a format change. Base 256's extra gain only appears for keys that use the full byte range, and its nodes and chains are too large for the current slot design.
