#!/usr/bin/env ruby
# frozen_string_literal: true

# Add current directory to load path for standalone execution
$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))

puts "Cassandra Performance Benchmark Runner"
puts "=" * 50

# Check if Cassandra is running
def check_cassandra_connection
  require "socket"

  begin
    socket = TCPSocket.new("127.0.0.1", 9042)
    socket.close
    true
  rescue Errno::ECONNREFUSED
    false
  end
end

unless check_cassandra_connection
  puts "❌ Cassandra is not running on localhost:9042"
  puts ""
  puts "To start Cassandra using Docker:"
  puts "  docker-compose up -d"
  puts ""
  puts "Or start your local Cassandra installation"
  exit 1
end

puts "✅ Cassandra connection verified"
puts ""

# Check dependencies
begin
  require_relative "comparison_benchmark"
rescue LoadError => e
  puts "❌ Missing dependencies: #{e.message}"
  puts ""
  if e.message.include?("cassandra")
    puts "Install the cassandra-driver gem for benchmarks:"
    puts "  gem install cassandra-driver"
    puts ""
    puts "This gem is not included as a default dependency."
  elsif e.message.include?("benchmark/ips")
    puts "Install the benchmark-ips gem:"
    puts "  gem install benchmark-ips"
  else
    puts "Missing required gems. Install with:"
    puts "  gem install benchmark-ips cassandra-driver"
  end
  exit 1
end

puts "✅ All dependencies available"
puts ""

# Run benchmarks
begin
  puts "Starting comprehensive performance benchmarks..."
  puts "This will take approximately 5-10 minutes to complete."
  puts ""

  runner = BenchmarkRunner.new
  runner.run_all_benchmarks

  puts ""
  puts "🎉 Benchmarks completed successfully!"
rescue Interrupt
  puts ""
  puts "⚠️  Benchmark interrupted by user"
rescue => e
  puts ""
  puts "❌ Benchmark failed: #{e.message}"
  puts e.backtrace.first(3).join("\n") if ENV["DEBUG"]
  exit 1
end
