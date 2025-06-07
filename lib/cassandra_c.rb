# frozen_string_literal: true

require_relative "cassandra_c/version"
require_relative "cassandra_c/cassandra_c"
require_relative "cassandra_c/types"

module CassandraC
  # Native module contains the low-level C++ driver bindings
  # These classes provide direct access to the underlying Cassandra C++ driver
  # For a more Ruby-idiomatic interface, use the classes in the CassandraC namespace
  module Native
    # All native classes are defined in the C extension

    # Add convenience methods to Session for batch operations
    class Session
      # Execute a batch of statements
      # @param type [Symbol] Batch type: :logged, :unlogged, or :counter
      # @param statements [Array<Statement, String>] Array of statements to batch
      # @param options [Hash] Execution options (e.g., async: true)
      # @return [Result, Future] Result object or Future if async
      def batch(type = :logged, statements = [], **options)
        batch_obj = Batch.new(type)

        statements.each do |stmt|
          if stmt.is_a?(String)
            # Convert string to Statement
            statement = Statement.new(stmt)
            batch_obj.add(statement)
          elsif stmt.is_a?(Statement)
            batch_obj.add(stmt)
          else
            raise ArgumentError, "Statement must be a String or Statement object"
          end
        end

        execute_batch(batch_obj, **options)
      end

      # Execute a logged batch (default - provides atomicity across partitions)
      def logged_batch(statements = [], **options)
        batch(:logged, statements, **options)
      end

      # Execute an unlogged batch (faster, no atomicity guarantee)
      def unlogged_batch(statements = [], **options)
        batch(:unlogged, statements, **options)
      end

      # Execute a counter batch (for counter updates only)
      def counter_batch(statements = [], **options)
        batch(:counter, statements, **options)
      end
    end

    # Add convenience methods to Batch class
    class Batch
      # Add multiple statements at once
      # @param statements [Array<Statement, String>] Array of statements to add
      def add_all(statements)
        statements.each { |stmt| add(stmt) }
        self
      end

      # Provide a block-based interface for building batches
      # @yield [batch] Block that receives the batch instance
      def self.build(type = :logged)
        batch = new(type)
        yield(batch) if block_given?
        batch
      end
    end
  end
end
