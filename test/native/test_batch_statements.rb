# frozen_string_literal: true

require "test_helper"

class TestBatchStatements < Minitest::Test
  def setup
    # Clean up test tables before each test
    session.query("TRUNCATE cassandra_c_test.test_types")
    session.query("TRUNCATE cassandra_c_test.single_counter")
    session.query("TRUNCATE cassandra_c_test.counter_table")
  end

  def test_batch_class_constants
    # Test that batch type constants are defined
    assert_equal 0, CassandraC::Native::Batch::LOGGED
    assert_equal 1, CassandraC::Native::Batch::UNLOGGED
    assert_equal 2, CassandraC::Native::Batch::COUNTER
  end

  def test_create_logged_batch
    batch = CassandraC::Native::Batch.new(:logged)
    assert_instance_of CassandraC::Native::Batch, batch
  end

  def test_create_unlogged_batch
    batch = CassandraC::Native::Batch.new(:unlogged)
    assert_instance_of CassandraC::Native::Batch, batch
  end

  def test_create_counter_batch
    batch = CassandraC::Native::Batch.new(:counter)
    assert_instance_of CassandraC::Native::Batch, batch
  end

  def test_create_batch_with_integer_type
    batch = CassandraC::Native::Batch.new(CassandraC::Native::Batch::LOGGED)
    assert_instance_of CassandraC::Native::Batch, batch
  end

  def test_create_batch_with_invalid_type
    assert_raises(ArgumentError) do
      CassandraC::Native::Batch.new(:invalid)
    end
  end

  def test_add_statement_to_batch
    batch = CassandraC::Native::Batch.new(:logged)
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "test1")
    statement.bind_by_index(1, "Test Name 1")

    batch.add(statement)

    # Batch should accept the statement without error
    assert_instance_of CassandraC::Native::Batch, batch
  end

  def test_execute_logged_batch_with_inserts
    batch = CassandraC::Native::Batch.new(:logged)

    # Create and add first statement
    statement1 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement1.bind_by_index(0, "test1")
    statement1.bind_by_index(1, "Test Name 1")
    batch.add(statement1)

    # Create and add second statement
    statement2 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, "test2")
    statement2.bind_by_index(1, "Test Name 2")
    batch.add(statement2)

    # Execute the batch
    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the inserts worked
    verify_result = session.query("SELECT id, text_col FROM cassandra_c_test.test_types WHERE id IN ('test1', 'test2')")
    rows = verify_result.to_a
    assert_equal 2, rows.length

    # Sort by id for consistent testing
    rows.sort_by! { |row| row[0] }
    assert_equal "test1", rows[0][0]
    assert_equal "Test Name 1", rows[0][1]
    assert_equal "test2", rows[1][0]
    assert_equal "Test Name 2", rows[1][1]
  end

  def test_execute_unlogged_batch_with_inserts
    batch = CassandraC::Native::Batch.new(:unlogged)

    # Create and add statements
    statement1 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement1.bind_by_index(0, "unlogged1")
    statement1.bind_by_index(1, "Unlogged 1")
    batch.add(statement1)

    statement2 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, "unlogged2")
    statement2.bind_by_index(1, "Unlogged 2")
    batch.add(statement2)

    # Execute the batch
    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the inserts worked
    verify_result = session.query("SELECT id, text_col FROM cassandra_c_test.test_types WHERE id IN ('unlogged1', 'unlogged2')")
    rows = verify_result.to_a
    assert_equal 2, rows.length
  end

  def test_execute_counter_batch
    batch = CassandraC::Native::Batch.new(:counter)

    # Create counter update statements
    statement1 = CassandraC::Native::Statement.new("UPDATE cassandra_c_test.single_counter SET count = count + ? WHERE id = ?", 2)
    statement1.bind_by_index(0, 10.to_cassandra_bigint)
    statement1.bind_by_index(1, "counter1")
    batch.add(statement1)

    statement2 = CassandraC::Native::Statement.new("UPDATE cassandra_c_test.single_counter SET count = count + ? WHERE id = ?", 2)
    statement2.bind_by_index(0, 20.to_cassandra_bigint)
    statement2.bind_by_index(1, "counter2")
    batch.add(statement2)

    # Execute the batch
    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the counter updates worked
    verify_result1 = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'counter1'")
    verify_result2 = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'counter2'")

    row1 = verify_result1.to_a.first
    row2 = verify_result2.to_a.first

    assert_equal 10, row1[0].to_i
    assert_equal 20, row2[0].to_i
  end

  def test_batch_with_consistency_level
    batch = CassandraC::Native::Batch.new(:logged)
    batch.consistency = :one

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "consistency_test")
    statement.bind_by_index(1, "Consistency Test")
    batch.add(statement)

    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the insert worked
    verify_result = session.query("SELECT text_col FROM cassandra_c_test.test_types WHERE id = 'consistency_test'")
    row = verify_result.to_a.first
    assert_equal "Consistency Test", row[0]
  end

  def test_batch_with_serial_consistency
    batch = CassandraC::Native::Batch.new(:logged)
    batch.serial_consistency = :serial

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "serial_test")
    statement.bind_by_index(1, "Serial Test")
    batch.add(statement)

    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result
  end

  def test_batch_with_timestamp
    batch = CassandraC::Native::Batch.new(:logged)
    timestamp = (Time.now.to_f * 1_000_000).to_i
    batch.timestamp = timestamp

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "timestamp_test")
    statement.bind_by_index(1, "Timestamp Test")
    batch.add(statement)

    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result
  end

  def test_batch_with_request_timeout
    batch = CassandraC::Native::Batch.new(:logged)
    batch.request_timeout = 30000  # 30 seconds

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "timeout_test")
    statement.bind_by_index(1, "Timeout Test")
    batch.add(statement)

    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result
  end

  def test_batch_with_idempotent_flag
    batch = CassandraC::Native::Batch.new(:logged)
    batch.idempotent = true

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "idempotent_test")
    statement.bind_by_index(1, "Idempotent Test")
    batch.add(statement)

    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result
  end

  def test_batch_with_mixed_operations
    batch = CassandraC::Native::Batch.new(:logged)

    # Insert
    statement1 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement1.bind_by_index(0, "mixed1")
    statement1.bind_by_index(1, "Mixed Operation 1")
    batch.add(statement1)

    # Another insert
    statement2 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, "mixed2")
    statement2.bind_by_index(1, "Mixed Operation 2")
    batch.add(statement2)

    # Execute the batch
    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result

    # Verify both inserts worked
    verify_result = session.query("SELECT id, text_col FROM cassandra_c_test.test_types WHERE id IN ('mixed1', 'mixed2')")
    rows = verify_result.to_a
    assert_equal 2, rows.length
  end

  def test_async_batch_execution
    batch = CassandraC::Native::Batch.new(:logged)

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "async_test")
    statement.bind_by_index(1, "Async Test")
    batch.add(statement)

    # Execute asynchronously
    future = session.execute_batch(batch, async: true)
    assert_instance_of CassandraC::Native::Future, future

    # Wait for completion and get result
    result = future.get_result
    assert_instance_of CassandraC::Native::Result, result

    # Verify the insert worked
    verify_result = session.query("SELECT text_col FROM cassandra_c_test.test_types WHERE id = 'async_test'")
    row = verify_result.to_a.first
    assert_equal "Async Test", row[0]
  end

  def test_convenience_batch_method
    # Test the convenience batch method on session
    statements = [
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('conv1', 'Convenience 1')",
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('conv2', 'Convenience 2')"
    ]

    result = session.batch(:logged, statements)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the inserts worked
    verify_result = session.query("SELECT id, text_col FROM cassandra_c_test.test_types WHERE id IN ('conv1', 'conv2')")
    rows = verify_result.to_a
    assert_equal 2, rows.length
  end

  def test_logged_batch_convenience_method
    statements = [
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('logged1', 'Logged 1')",
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('logged2', 'Logged 2')"
    ]

    result = session.logged_batch(statements)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the inserts worked
    verify_result = session.query("SELECT id, text_col FROM cassandra_c_test.test_types WHERE id IN ('logged1', 'logged2')")
    rows = verify_result.to_a
    assert_equal 2, rows.length
  end

  def test_unlogged_batch_convenience_method
    statements = [
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('unlogged_conv1', 'Unlogged Conv 1')",
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('unlogged_conv2', 'Unlogged Conv 2')"
    ]

    result = session.unlogged_batch(statements)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the inserts worked
    verify_result = session.query("SELECT id, text_col FROM cassandra_c_test.test_types WHERE id IN ('unlogged_conv1', 'unlogged_conv2')")
    rows = verify_result.to_a
    assert_equal 2, rows.length
  end

  def test_counter_batch_convenience_method
    statements = [
      "UPDATE cassandra_c_test.single_counter SET count = count + 5 WHERE id = 'conv_counter1'",
      "UPDATE cassandra_c_test.single_counter SET count = count + 15 WHERE id = 'conv_counter2'"
    ]

    result = session.counter_batch(statements)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the counter updates worked
    verify_result1 = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'conv_counter1'")
    verify_result2 = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'conv_counter2'")

    row1 = verify_result1.to_a.first
    row2 = verify_result2.to_a.first

    assert_equal 5, row1[0].to_i
    assert_equal 15, row2[0].to_i
  end

  def test_batch_build_with_block
    result = nil

    CassandraC::Native::Batch.build(:logged) do |batch|
      statement1 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
      statement1.bind_by_index(0, "block1")
      statement1.bind_by_index(1, "Block Test 1")
      batch.add(statement1)

      statement2 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
      statement2.bind_by_index(0, "block2")
      statement2.bind_by_index(1, "Block Test 2")
      batch.add(statement2)

      result = session.execute_batch(batch)
    end

    assert_instance_of CassandraC::Native::Result, result

    # Verify the inserts worked
    verify_result = session.query("SELECT id, text_col FROM cassandra_c_test.test_types WHERE id IN ('block1', 'block2')")
    rows = verify_result.to_a
    assert_equal 2, rows.length
  end

  def test_empty_batch_execution
    batch = CassandraC::Native::Batch.new(:logged)

    # Execute empty batch - should work but do nothing
    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result
  end

  def test_large_batch_execution
    batch = CassandraC::Native::Batch.new(:unlogged)

    # Add many statements (but within reasonable limits)
    50.times do |i|
      statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
      statement.bind_by_index(0, "large_batch_#{i}")
      statement.bind_by_index(1, "Large Batch Item #{i}")
      batch.add(statement)
    end

    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result

    # Verify some of the inserts worked by checking a specific row
    verify_result = session.query("SELECT text_col FROM cassandra_c_test.test_types WHERE id = 'large_batch_0'")
    row = verify_result.to_a.first
    assert_equal "Large Batch Item 0", row[0]
  end
end
