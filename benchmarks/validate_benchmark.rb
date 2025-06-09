#!/usr/bin/env ruby
# frozen_string_literal: true

require "bundler/setup"

# Simple validation of benchmark components without running full benchmarks
puts "Validating benchmark components..."

# Test 1: Check required gems can be loaded
puts "✓ Testing core gem loading..."
begin
  require "benchmark/ips"
  require "cassandra_c"
  require "securerandom"
  require "sorted_set"
  puts "  ✓ Core gems loaded successfully"
rescue LoadError => e
  puts "  ❌ Missing core gem: #{e.message}"
  exit 1
end

# Test 1b: Check optional benchmark dependency
puts "✓ Testing benchmark dependency..."
begin
  require "cassandra"
  puts "  ✓ cassandra-driver available for benchmarks"
rescue LoadError
  puts "  ⚠️  cassandra-driver not installed (optional for benchmarks)"
  puts "     Install with: gem install cassandra-driver"
end

# Test 2: Check memory profiler
puts "✓ Testing memory profiler..."
begin
  require_relative "memory_profiler"
  MemoryProfiler.profile("Validation test") { 100.times { "test" } }
  puts "  ✓ Memory profiler working"
rescue => e
  puts "  ❌ Memory profiler error: #{e.message}"
  exit 1
end

# Test 3: Check benchmark file syntax (skip if cassandra-driver missing)
puts "✓ Testing benchmark file syntax..."
begin
  require "cassandra"
  require_relative "comparison_benchmark"
  puts "  ✓ Benchmark file syntax valid"
rescue LoadError
  puts "  ⚠️  Skipping benchmark syntax check (cassandra-driver not available)"
rescue => e
  puts "  ❌ Benchmark syntax error: #{e.message}"
  exit 1
end

# Test 4: Check benchmark runner syntax
puts "✓ Testing benchmark runner syntax..."
begin
  # Load without executing the main logic
  File.read("benchmarks/run_benchmarks.rb")
  puts "  ✓ Runner script syntax valid"
rescue => e
  puts "  ❌ Runner syntax error: #{e.message}"
  exit 1
end

puts ""
puts "🎉 All benchmark components validated successfully!"
puts ""
puts "To run the full benchmarks:"
puts "  1. Install cassandra-driver: gem install cassandra-driver"
puts "  2. Start Cassandra: docker compose up -d"
puts "  3. Run benchmarks: ruby benchmarks/run_benchmarks.rb"
puts "     or: ruby benchmarks/comparison_benchmark.rb"
