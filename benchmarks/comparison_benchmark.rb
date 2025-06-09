#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"
require "benchmark/ips"
require "cassandra_c"
require "securerandom"

require "sorted_set"

begin
  require "cassandra"
rescue LoadError
  puts "❌ cassandra-driver gem not found."
  puts ""
  puts "To run benchmarks, install the cassandra-driver gem:"
  puts "  gem install cassandra-driver"
  puts "  # or add to Gemfile: gem 'cassandra-driver', '~> 3.2'"
  puts ""
  puts "This gem is not included as a default dependency to keep"
  puts "the CassandraC installation lightweight."
  exit 1
end

# Benchmark configuration
CASSANDRA_HOSTS = ["127.0.0.1"].freeze
ITERATIONS = 1000
SAMPLE_DATA = {
  text_data: "The quick brown fox jumps over the lazy dog",
  int_data: 42,
  bigint_data: 9_223_372_036_854_775_807,
  float_data: 3.14159,
  boolean_data: true,
  list_data: [1, 2, 3, 4, 5],
  set_data: Set.new([1, 2, 3, 4, 5]),
  map_data: {"key1" => "value1", "key2" => "value2", "key3" => "value3"}
}.freeze

class BenchmarkRunner
  def initialize
    @results = {}
    setup_cassandra_connections
    setup_test_data
  end

  def run_all_benchmarks
    puts "=" * 80
    puts "CassandraC vs cassandra-driver Performance Comparison"
    puts "=" * 80
    puts

    benchmark_connection_setup
    benchmark_basic_operations
    benchmark_prepared_statements  
    benchmark_batch_operations
    benchmark_type_conversions
    benchmark_collection_operations
    benchmark_result_processing
    benchmark_concurrent_operations
    benchmark_memory_usage

    print_summary
  end

  private

  def setup_cassandra_connections
    puts "Setting up connections..."
    
    # CassandraC setup
    @cassandra_c_cluster = CassandraC::Native::Cluster.new
    @cassandra_c_cluster.contact_points = CASSANDRA_HOSTS.join(",")
    @cassandra_c_cluster.port = 9042
    
    @cassandra_c_session = CassandraC::Native::Session.new
    @cassandra_c_session.connect(@cassandra_c_cluster)

    # cassandra-driver setup  
    @cassandra_driver_cluster = Cassandra.cluster(hosts: CASSANDRA_HOSTS)
    @cassandra_driver_session = @cassandra_driver_cluster.connect

    puts "Connections established"
  rescue => e
    puts "Error setting up connections: #{e.message}"
    puts "Make sure Cassandra is running on #{CASSANDRA_HOSTS.join(', ')}"
    exit 1
  end

  def setup_test_data
    puts "Setting up test keyspace and tables..."
    
    keyspace_cql = <<~SQL
      CREATE KEYSPACE IF NOT EXISTS benchmark_test 
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    SQL

    drop_table_cql = "DROP TABLE IF EXISTS benchmark_test.test_table"
    
    table_cql = <<~SQL
      CREATE TABLE benchmark_test.test_table (
        id TEXT PRIMARY KEY,
        text_col TEXT,
        int_col INT,
        bigint_col BIGINT,
        float_col FLOAT,
        boolean_col BOOLEAN,
        list_col LIST<INT>,
        set_col SET<INT>,
        map_col MAP<TEXT, TEXT>
      )
    SQL

    # Execute on both drivers
    [@cassandra_c_session, @cassandra_driver_session].each do |session|
      session.execute(keyspace_cql)
      session.execute(drop_table_cql)
      session.execute(table_cql)
    end

    # Switch to benchmark keyspace
    @cassandra_c_session.execute("USE benchmark_test")
    @cassandra_driver_session.execute("USE benchmark_test")

    puts "Test environment ready"
  rescue => e
    puts "Error setting up test data: #{e.message}"
    exit 1
  end

  def benchmark_connection_setup
    puts "\n" + "=" * 50
    puts "1. Connection Setup Performance"
    puts "=" * 50

    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c cluster creation") do
        cluster = CassandraC::Native::Cluster.new
        cluster.contact_points = "127.0.0.1"
        cluster.port = 9042
      end

      x.report("cassandra-driver cluster creation") do
        Cassandra.cluster(hosts: ["127.0.0.1"])
      end

      x.compare!
    end
  end

  def benchmark_basic_operations
    puts "\n" + "=" * 50  
    puts "2. Basic Query Operations"
    puts "=" * 50

    simple_insert = "INSERT INTO test_table (id, text_col) VALUES (?, ?)"
    simple_select = "SELECT * FROM test_table LIMIT 1"

    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c simple insert") do
        stmt = CassandraC::Native::Statement.new(simple_insert, 2)
        stmt.bind_by_index(0, SecureRandom.uuid.to_s)
        stmt.bind_by_index(1, SAMPLE_DATA[:text_data])
        @cassandra_c_session.execute(stmt)
      end

      x.report("cassandra-driver simple insert") do
        @cassandra_driver_session.execute(simple_insert, 
          arguments: [SecureRandom.uuid.to_s, SAMPLE_DATA[:text_data]])
      end

      x.report("cassandra_c simple select") do
        @cassandra_c_session.execute(simple_select)
      end

      x.report("cassandra-driver simple select") do
        @cassandra_driver_session.execute(simple_select)
      end

      x.compare!
    end
  end

  def benchmark_prepared_statements
    puts "\n" + "=" * 50
    puts "3. Prepared Statement Performance"  
    puts "=" * 50

    insert_cql = <<~SQL
      INSERT INTO test_table (id, text_col, int_col, bigint_col, float_col, boolean_col)
      VALUES (?, ?, ?, ?, ?, ?)
    SQL

    # Prepare statements
    cassandra_c_prepared = @cassandra_c_session.prepare(insert_cql)
    cassandra_driver_prepared = @cassandra_driver_session.prepare(insert_cql)

    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c prepared execute") do
        stmt = cassandra_c_prepared.bind([
          SecureRandom.uuid.to_s,
          SAMPLE_DATA[:text_data],
          SAMPLE_DATA[:int_data],
          SAMPLE_DATA[:bigint_data],
          SAMPLE_DATA[:float_data].to_cassandra_float,
          SAMPLE_DATA[:boolean_data]
        ])
        @cassandra_c_session.execute(stmt)
      end

      x.report("cassandra-driver prepared execute") do
        @cassandra_driver_session.execute(cassandra_driver_prepared, arguments: [
          SecureRandom.uuid.to_s,
          SAMPLE_DATA[:text_data],
          SAMPLE_DATA[:int_data],
          SAMPLE_DATA[:bigint_data],
          SAMPLE_DATA[:float_data],
          SAMPLE_DATA[:boolean_data]
        ])
      end

      x.compare!
    end
  end

  def benchmark_batch_operations
    puts "\n" + "=" * 50
    puts "4. Batch Operation Performance"
    puts "=" * 50

    insert_cql = "INSERT INTO test_table (id, text_col, int_col) VALUES (?, ?, ?)"

    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c batch (10 statements)") do
        batch = CassandraC::Native::Batch.new(:unlogged)
        
        10.times do
          stmt = CassandraC::Native::Statement.new(insert_cql, 3)
          stmt.bind_by_index(0, SecureRandom.uuid.to_s)
          stmt.bind_by_index(1, SAMPLE_DATA[:text_data])
          stmt.bind_by_index(2, SAMPLE_DATA[:int_data].to_cassandra_int)
          batch.add(stmt)
        end
        
        @cassandra_c_session.execute_batch(batch)
      end

      x.report("cassandra-driver batch (10 statements)") do
        batch = @cassandra_driver_session.unlogged_batch
        
        10.times do
          batch.add(insert_cql, arguments: [
            SecureRandom.uuid.to_s,
            SAMPLE_DATA[:text_data], 
            SAMPLE_DATA[:int_data]
          ])
        end
        
        @cassandra_driver_session.execute(batch)
      end

      x.compare!
    end
  end

  def benchmark_type_conversions
    puts "\n" + "=" * 50
    puts "5. Type Conversion Performance"
    puts "=" * 50

    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c typed integers") do
        100.times do
          42.to_cassandra_tinyint
          1000.to_cassandra_smallint
          1_000_000.to_cassandra_int
          9_223_372_036_854_775_807.to_cassandra_bigint
        end
      end

      x.report("cassandra-driver type wrapping") do
        100.times do
          # cassandra-driver doesn't have explicit type wrapping for basic types
          # This represents the overhead of their internal type handling
          [42, 1000, 1_000_000, 9_223_372_036_854_775_807].each(&:to_s)
        end
      end

      x.report("cassandra_c typed floats") do
        100.times do
          3.14.to_cassandra_float
          2.718281828.to_cassandra_double
          BigDecimal("123.456").to_cassandra_decimal
        end
      end

      x.report("cassandra-driver float handling") do
        100.times do
          [3.14, 2.718281828, BigDecimal("123.456")].each(&:to_s)
        end
      end

      x.compare!
    end
  end

  def benchmark_collection_operations
    puts "\n" + "=" * 50
    puts "6. Collection Operations Performance"
    puts "=" * 50

    collection_insert = <<~SQL
      INSERT INTO test_table (id, list_col, set_col, map_col) VALUES (?, ?, ?, ?)
    SQL

    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c collections") do
        stmt = CassandraC::Native::Statement.new(collection_insert, 4)
        stmt.bind_by_index(0, SecureRandom.uuid.to_s)
        stmt.bind_by_index(1, SAMPLE_DATA[:list_data])
        stmt.bind_by_index(2, SAMPLE_DATA[:set_data])
        stmt.bind_by_index(3, SAMPLE_DATA[:map_data])
        @cassandra_c_session.execute(stmt)
      end

      x.report("cassandra-driver collections") do
        @cassandra_driver_session.execute(collection_insert, arguments: [
          SecureRandom.uuid.to_s,
          SAMPLE_DATA[:list_data],
          SAMPLE_DATA[:set_data].to_a, # cassandra-driver expects arrays for sets
          SAMPLE_DATA[:map_data]
        ])
      end

      x.compare!
    end
  end

  def benchmark_result_processing
    puts "\n" + "=" * 50
    puts "7. Result Processing Performance"
    puts "=" * 50

    # Insert test data first
    insert_cql = "INSERT INTO test_table (id, text_col, int_col, bigint_col) VALUES (?, ?, ?, ?)"
    
    # Insert 100 rows for testing
    100.times do |i|
      stmt = CassandraC::Native::Statement.new(insert_cql, 4)
      stmt.bind_by_index(0, "test_#{i}")
      stmt.bind_by_index(1, "Sample text #{i}")
      stmt.bind_by_index(2, i.to_cassandra_int)
      stmt.bind_by_index(3, (i * 1000).to_cassandra_bigint)
      @cassandra_c_session.execute(stmt)
    end

    select_cql = "SELECT * FROM test_table LIMIT 50"

    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c result iteration") do
        result = @cassandra_c_session.execute(select_cql)
        count = 0
        result.each_row do |row|
          count += 1
          row.column_value_by_name("id")
          row.column_value_by_name("text_col")
          row.column_value_by_name("int_col")
        end
      end

      x.report("cassandra-driver result iteration") do
        result = @cassandra_driver_session.execute(select_cql)
        count = 0
        result.each do |row|
          count += 1
          row["id"]
          row["text_col"] 
          row["int_col"]
        end
      end

      x.report("cassandra_c result to array") do
        result = @cassandra_c_session.execute(select_cql)
        result.to_a
      end

      x.report("cassandra-driver result to array") do
        result = @cassandra_driver_session.execute(select_cql)
        result.to_a
      end

      x.compare!
    end
  end

  def benchmark_concurrent_operations
    puts "\n" + "=" * 50
    puts "8. Concurrent Operations Performance"
    puts "=" * 50

    insert_cql = "INSERT INTO test_table (id, text_col, int_col) VALUES (?, ?, ?)"
    
    Benchmark.ips do |x|
      x.config(time: 5, warmup: 2)

      x.report("cassandra_c concurrent inserts (4 threads)") do
        threads = []
        4.times do |thread_id|
          threads << Thread.new do
            5.times do |i|
              stmt = CassandraC::Native::Statement.new(insert_cql, 3)
              stmt.bind_by_index(0, "thread_#{thread_id}_#{i}_#{SecureRandom.uuid}")
              stmt.bind_by_index(1, "Concurrent text #{thread_id}")
              stmt.bind_by_index(2, i.to_cassandra_int)
              @cassandra_c_session.execute(stmt)
            end
          end
        end
        threads.each(&:join)
      end

      x.report("cassandra-driver concurrent inserts (4 threads)") do
        threads = []
        4.times do |thread_id|
          threads << Thread.new do
            5.times do |i|
              @cassandra_driver_session.execute(insert_cql, arguments: [
                "thread_#{thread_id}_#{i}_#{SecureRandom.uuid}",
                "Concurrent text #{thread_id}",
                i
              ])
            end
          end
        end
        threads.each(&:join)
      end

      x.compare!
    end
  end

  def benchmark_memory_usage
    puts "\n" + "=" * 50
    puts "9. Memory Usage Comparison"
    puts "=" * 50

    require_relative "memory_profiler"

    puts "Testing statement creation memory overhead..."

    MemoryProfiler.profile("CassandraC Statement Creation") do
      1000.times do
        stmt = CassandraC::Native::Statement.new("SELECT * FROM test_table WHERE id = ?", 1)
        stmt.bind_by_index(0, SecureRandom.uuid.to_s)
      end
    end

    MemoryProfiler.profile("cassandra-driver Equivalent Operations") do
      1000.times do
        # Simulate equivalent operations (cassandra-driver doesn't pre-create statements)
        query = "SELECT * FROM test_table WHERE id = ?"
        args = [SecureRandom.uuid.to_s]
      end
    end

    puts "\nTesting batch operation memory usage..."

    MemoryProfiler.profile("CassandraC Batch Operations") do
      10.times do
        batch = CassandraC::Native::Batch.new(:unlogged)
        20.times do
          stmt = CassandraC::Native::Statement.new("INSERT INTO test_table (id, text_col) VALUES (?, ?)", 2)
          stmt.bind_by_index(0, SecureRandom.uuid.to_s)
          stmt.bind_by_index(1, "test")
          batch.add(stmt)
        end
        @cassandra_c_session.execute_batch(batch)
      end
    end

    MemoryProfiler.profile("cassandra-driver Batch Operations") do
      10.times do
        batch = @cassandra_driver_session.unlogged_batch
        20.times do
          batch.add("INSERT INTO test_table (id, text_col) VALUES (?, ?)", 
                   arguments: [SecureRandom.uuid.to_s, "test"])
        end
        @cassandra_driver_session.execute(batch)
      end
    end
  end

  def print_summary
    puts "\n" + "=" * 80
    puts "BENCHMARK SUMMARY"
    puts "=" * 80
    puts
    puts "Key Performance Areas Tested:"
    puts "  ✓ Connection and cluster setup"
    puts "  ✓ Basic query operations (INSERT/SELECT)"
    puts "  ✓ Prepared statement execution"
    puts "  ✓ Batch operation performance"
    puts "  ✓ Type conversion overhead"
    puts "  ✓ Collection handling (Lists, Sets, Maps)"
    puts "  ✓ Result processing and iteration"
    puts "  ✓ Concurrent operations (multi-threaded)"
    puts "  ✓ Memory usage patterns and allocation overhead"
    puts
    puts "The results above show iterations per second (higher is better)."
    puts "Look for 'x.xx faster' comparisons to see relative performance."
    puts
    puts "Expected advantages of CassandraC:"
    puts "  • Lower memory allocation overhead"
    puts "  • Faster type conversions (C vs Ruby)"
    puts "  • More efficient prepared statement binding"
    puts "  • Reduced GC pressure from fewer object allocations"
    puts
    puts "Note: Results may vary based on system performance, Cassandra load,"
    puts "and network conditions. Run multiple times for consistent baselines."
  end
end

# Run benchmarks if this file is executed directly
if __FILE__ == $0
  puts "Starting Cassandra Performance Benchmarks..."
  puts "Ensure Cassandra is running on localhost:9042"
  puts

  begin
    runner = BenchmarkRunner.new
    runner.run_all_benchmarks
  rescue Interrupt
    puts "\nBenchmark interrupted by user"
  rescue => e
    puts "Benchmark failed: #{e.message}"
    puts e.backtrace.first(5).join("\n")
  end
end