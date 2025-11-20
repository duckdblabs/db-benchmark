# Haskell DataFrame Benchmark

This benchmark entry uses Haskell with the `mchav/dataframe` library to implement dataframe operations.

## Implementation Details

- **Language**: Haskell (GHC)
- **DataFrame Library**: [mchav/dataframe](https://github.com/mchav/dataframe)
- **Build Tool**: Stack

## About mchav/dataframe

The `dataframe` library is a fast, safe, and intuitive DataFrame library for Haskell that provides:
- Type-safe column operations
- Familiar operations for users coming from pandas, dplyr, or polars
- Concise, declarative, and composable data pipelines
- Static typing that catches many bugs at compile time

Resources:
- GitHub: https://github.com/mchav/dataframe
- Hackage: https://hackage.haskell.org/package/dataframe
- Documentation: https://dataframe.readthedocs.io/

## Implemented Benchmarks

### Groupby (`groupby-haskell.hs`)
Implements 5 out of 10 groupby questions:
1. sum v1 by id1
2. sum v1 by id1:id2
3. sum v1 mean v3 by id3
4. mean v1:v3 by id4
5. sum v1:v3 by id6

Uses `D.groupBy` and `D.aggregate` with expression DSL (`F.sum`, `F.mean`).

Note: Questions 6-10 would require additional statistical functions (median, standard deviation, regression, top-n selection).

### Join (`join-haskell.hs`)
Implements all 5 join questions:
1. small inner on int
2. medium inner on int
3. medium outer on int (using leftJoin)
4. medium inner on factor
5. big inner on int

Uses `DJ.innerJoin` and `DJ.leftJoin` from `DataFrame.Operations.Join`.

## Setup

Run the setup script to install dependencies:
```bash
./haskell/setup-haskell.sh
```

This will:
1. Install Stack (if not present)
2. Initialize the Stack project
3. Build all dependencies
4. Compile the benchmark executables

## API Usage Examples

```haskell
-- Read CSV
df <- D.readCsv "data/file.csv"

-- GroupBy with aggregation
let grouped = D.groupBy ["id1"] df
let result = D.aggregate [F.sum (F.col @Double "v1") `F.as` "v1_sum"] grouped

-- Inner Join
let joined = DJ.innerJoin ["id1"] df1 df2

-- Get dimensions
let (rows, cols) = D.dimensions df
```

## Performance Notes

The implementation uses:
- Type-safe column operations with `TypeApplications`
- Expression DSL for clean aggregation syntax
- Efficient grouping and joining operations from the dataframe library

This benchmark demonstrates Haskell's capabilities for high-performance dataframe operations with the additional benefits of static typing and functional programming.
