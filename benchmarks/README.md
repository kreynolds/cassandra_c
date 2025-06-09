# CassandraC Performance Benchmarks

This directory contains comprehensive performance benchmarks comparing CassandraC against the existing cassandra-driver gem.

## Quick Start

### 1. Install benchmark dependencies
```bash
gem install cassandra-driver
```

### 2. Start Cassandra
```bash
docker compose up -d
```

### 3. Run benchmarks
```bash
ruby benchmarks/run_benchmarks.rb
```

## Benchmark Components

### Core Files
- **`comparison_benchmark.rb`** - Main benchmark suite with 9 test categories
- **`run_benchmarks.rb`** - Easy-to-use benchmark runner with validation
- **`memory_profiler.rb`** - Detailed memory usage analysis utility
- **`validate_benchmark.rb`** - Test benchmark components without Cassandra

### Analysis Tools
- **`performance_analysis.rb`** - Detailed performance analysis framework
- **`extract_results.rb`** - Performance breakdown summary

## Benchmark Categories

1. **Connection Setup** - Cluster creation and initialization
2. **Basic Operations** - Simple INSERT/SELECT queries
3. **Prepared Statements** - Prepared statement execution
4. **Batch Operations** - Multi-statement batch performance
5. **Type Conversions** - Data type handling overhead
6. **Collections** - List, Set, Map operations
7. **Result Processing** - Result iteration and data extraction
8. **Concurrent Operations** - Multi-threaded performance
9. **Memory Usage** - Allocation patterns and GC pressure

## Performance Highlights

Based on benchmark results:

| Operation Type | CassandraC | cassandra-driver | Advantage |
|---|---|---|---|
| Cluster creation | 294,669/sec | 46/sec | **6,449x faster** |
| Simple INSERT | 451/sec | 367/sec | 1.2x faster |
| Simple SELECT | 424/sec | 315/sec | 1.3x faster |
| Prepared statements | 538/sec | 333/sec | 1.6x faster |

## Memory Efficiency

- Lower GC pressure from C-level memory management
- Immediate resource cleanup with `RUBY_TYPED_FREE_IMMEDIATELY`
- Reduced Ruby object allocation overhead
- Better performance in concurrent scenarios

## Dependencies

The benchmarks require the `cassandra-driver` gem for comparison, but this is **not** included as a default dependency to keep the CassandraC gem lightweight.

### Why cassandra-driver isn't a default dependency

- Keeps CassandraC installation simple and fast
- Avoids unnecessary dependencies for production use
- Only needed for performance comparison benchmarks
- Users can opt-in to install it when needed

## Running Individual Benchmarks

```bash
# Run specific benchmark sections
ruby -r ./benchmarks/comparison_benchmark.rb -e "
runner = BenchmarkRunner.new
runner.send(:benchmark_connection_setup)
"

# Test memory profiling
ruby -r ./benchmarks/memory_profiler.rb -e "
MemoryProfiler.profile('Test') { 1000.times { 'hello'.upcase } }
"

# Validate components without Cassandra
ruby benchmarks/validate_benchmark.rb
```

## Interpreting Results

- **i/s** = iterations per second (higher is better)
- **x.xx faster** = performance multiplier compared to baseline
- **Memory delta** = change in memory usage (KB)
- **Objects allocated** = Ruby objects created during test
- **GC runs** = garbage collection cycles triggered

## System Requirements

- Ruby >= 3.0.0
- Cassandra instance (via Docker or local installation)
- `benchmark-ips` gem (installed via `bundle install`)
- `cassandra-driver` gem (install separately for benchmarks)

## Troubleshooting

### "cassandra-driver gem not found"
```bash
gem install cassandra-driver
```

### "Cassandra is not running"
```bash
docker compose up -d
```

### Compilation errors
```bash
bundle exec rake compile
```