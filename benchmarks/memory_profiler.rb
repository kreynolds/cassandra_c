#!/usr/bin/env ruby
# frozen_string_literal: true

require "benchmark"

# Memory profiling utilities for benchmarking
class MemoryProfiler
  def self.profile(description = "Memory Profile", &block)
    puts "\n--- #{description} ---"

    # Force GC before measurement
    GC.start
    GC.compact if GC.respond_to?(:compact)

    # Capture initial state
    initial_memory = memory_usage_kb
    initial_gc_stats = GC.stat.dup
    initial_objects = ObjectSpace.count_objects.dup

    start_time = Time.now

    # Execute the block
    result = yield

    end_time = Time.now

    # Force GC again to measure actual retained memory
    GC.start

    # Capture final state
    final_memory = memory_usage_kb
    final_gc_stats = GC.stat
    final_objects = ObjectSpace.count_objects

    # Calculate deltas
    memory_delta = final_memory - initial_memory
    execution_time = end_time - start_time

    # Object allocation stats
    objects_allocated = final_gc_stats[:total_allocated_objects] - initial_gc_stats[:total_allocated_objects]
    objects_freed = final_gc_stats[:total_freed_objects] - initial_gc_stats[:total_freed_objects]
    gc_runs = final_gc_stats[:count] - initial_gc_stats[:count]

    # Object type breakdown
    object_types = {}
    initial_objects.each do |type, initial_count|
      final_count = final_objects[type] || 0
      delta = final_count - initial_count
      object_types[type] = delta if delta != 0
    end

    # Report results
    puts "Execution time: #{(execution_time * 1000).round(2)}ms"
    puts "Memory delta: #{(memory_delta > 0) ? "+" : ""}#{memory_delta} KB"
    puts "Objects allocated: #{objects_allocated}"
    puts "Objects freed: #{objects_freed}"
    puts "Net objects retained: #{objects_allocated - objects_freed}"
    puts "GC runs triggered: #{gc_runs}"

    if object_types.any?
      puts "\nObject type changes:"
      object_types.sort_by { |_type, delta| -delta.abs }.first(5).each do |type, delta|
        puts "  #{type}: #{(delta > 0) ? "+" : ""}#{delta}"
      end
    end

    puts "Memory efficiency: #{(objects_allocated > 0) ? (memory_delta.to_f / objects_allocated * 1024).round(2) : 0} bytes/object"
    puts "-" * 50

    result
  end

  def self.compare_memory(name1, name2, &blocks)
    puts "\n" + "=" * 60
    puts "MEMORY COMPARISON: #{name1} vs #{name2}"
    puts "=" * 60

    results = {}

    blocks.each_with_index do |(name, block), index|
      results[name] = profile(name, &block)
    end

    puts "\nComparison Summary:"
    puts "TODO: Add detailed comparison logic"

    results
  end

  class << self
    private

    def memory_usage_kb
      if RUBY_PLATFORM.match?(/darwin|mac os/)
        # macOS
        `ps -o rss= -p #{Process.pid}`.to_i
      elsif RUBY_PLATFORM.match?(/linux/)
        # Linux - read from /proc/self/status
        status = File.read("/proc/self/status")
        if (match = status.match(/VmRSS:\s+(\d+) kB/))
          match[1].to_i
        else
          0
        end
      else
        # Fallback: try ps command
        `ps -o rss= -p #{Process.pid}`.to_i
      end
    rescue
      0
    end
  end
end
