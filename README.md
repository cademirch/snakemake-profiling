# Snakemake Persistence Benchmarking

This repository benchmarks different persistence implementations in Snakemake. It compares the performance of JSON-based persistence against LMDB-based persistence implementations across various workflow patterns and scenarios. 


## Persistence Implementations

The benchmarks compare three persistence modes:

- **json**: Original JSON-based metadata storage (baseline)
- **lmdb**: LMDB-based persistence without mtime tracking
- **lmdb_mtime**: LMDB-based persistence with mtime tracking

Controlled via environment variables:
- `SNAKEMAKE_USE_LMDB_PERSISTENCE`: Enable LMDB persistence (0 or 1)
- `SNAKEMAKE_USE_LMDB_PERSISTENCE_MTIME`: Enable mtime tracking (0 or 1)

## Workflows

Three workflow patterns are benchmarked:

- **linear**: Sequential chain of rules
- **diamond**: Diamond-shaped dependency graph
- **fanout**: Single input branching to multiple outputs

## Scenarios

Each workflow is tested under three scenarios:

- **no_change**: Fresh DAG build with no metadata (baseline)
- **param_change**: DAG rebuild after parameter changes (with `--rerun-triggers params`)
- **code_change**: DAG rebuild after code changes (with `--rerun-triggers code`)

## Running Benchmarks

```bash
# Run all benchmarks (generates comparison.png and comparison.csv)
snakemake --cores all

# Run specific workflow/persistence combination
snakemake benchmarks/linear/lmdb/no_change.txt --cores all

# Clean all generated files
rm -rf benchmarks/ test_runs/ templates/ logs/ comparison.* .snakemake/
```

## Output

- `comparison.csv`: Aggregated benchmark data
- `comparison.png`: Visualization of results across workflows, persistence types, and scenarios