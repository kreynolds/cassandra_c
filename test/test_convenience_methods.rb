# frozen_string_literal: true

require "test_helper"

class TestConvenienceMethods < Minitest::Test
  def setup
    # Clean up test table before each test
    session.query("TRUNCATE cassandra_c_test.test_types")
  end

  # Test Batch.build method without block
  def test_batch_build_without_block
    batch = CassandraC::Native::Batch.build(:unlogged)
    assert_instance_of CassandraC::Native::Batch, batch

    # Add a statement and execute to verify the batch works
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "build_test")
    statement.bind_by_index(1, "Build Test")
    batch.add(statement)

    result = session.execute_batch(batch)
    assert_instance_of CassandraC::Native::Result, result

    # Verify the insert worked
    verify_result = session.query("SELECT text_col FROM cassandra_c_test.test_types WHERE id = 'build_test'")
    row = verify_result.to_a.first
    assert_equal "Build Test", row[0]
  end

  # Test Batch.build with default logged type
  def test_batch_build_default_type
    batch = CassandraC::Native::Batch.build
    assert_instance_of CassandraC::Native::Batch, batch
  end

  # Test the add_all method returns self for chaining
  def test_batch_add_all_returns_self
    batch = CassandraC::Native::Batch.new(:logged)

    statement1 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement1.bind_by_index(0, "chain1")
    statement1.bind_by_index(1, "Chain 1")

    statement2 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, "chain2")
    statement2.bind_by_index(1, "Chain 2")

    # Test that add_all returns self for method chaining
    returned_batch = batch.add_all([statement1, statement2])
    assert_equal batch, returned_batch
  end

  # Test batch convenience methods with async option
  def test_batch_convenience_methods_async
    # Test logged_batch with async
    statements = [
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('async_logged1', 'Async Logged 1')",
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('async_logged2', 'Async Logged 2')"
    ]

    future = session.logged_batch(statements, async: true)
    assert_instance_of CassandraC::Native::Future, future

    result = future.get_result
    assert_instance_of CassandraC::Native::Result, result

    # Test unlogged_batch with async
    statements = [
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('async_unlogged1', 'Async Unlogged 1')",
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('async_unlogged2', 'Async Unlogged 2')"
    ]

    future = session.unlogged_batch(statements, async: true)
    assert_instance_of CassandraC::Native::Future, future

    result = future.get_result
    assert_instance_of CassandraC::Native::Result, result
  end

  # Test empty statements array for convenience methods
  def test_convenience_methods_empty_statements
    # Test with empty statements arrays
    result = session.logged_batch([])
    assert_instance_of CassandraC::Native::Result, result

    result = session.unlogged_batch([])
    assert_instance_of CassandraC::Native::Result, result

    result = session.counter_batch([])
    assert_instance_of CassandraC::Native::Result, result
  end

  # Test convenience methods without specifying statements (default empty array)
  def test_convenience_methods_no_statements
    result = session.logged_batch
    assert_instance_of CassandraC::Native::Result, result

    result = session.unlogged_batch
    assert_instance_of CassandraC::Native::Result, result

    result = session.counter_batch
    assert_instance_of CassandraC::Native::Result, result
  end

  # Test generic batch method with all options
  def test_batch_method_with_options
    statements = [
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('batch_opts1', 'Batch Options 1')",
      "INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('batch_opts2', 'Batch Options 2')"
    ]

    # Test with async option
    future = session.batch(:logged, statements, async: true)
    assert_instance_of CassandraC::Native::Future, future
    result = future.get_result
    assert_instance_of CassandraC::Native::Result, result

    # Test with other options
    result = session.batch(:unlogged, statements, consistency: :one)
    assert_instance_of CassandraC::Native::Result, result
  end

  # Test batch convenience methods are actually called
  def test_batch_methods_called
    # This test ensures our convenience methods get executed for coverage
    session.logged_batch(["INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('coverage1', 'Coverage Test 1')"])
    session.unlogged_batch(["INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('coverage2', 'Coverage Test 2')"])
    session.counter_batch(["UPDATE cassandra_c_test.single_counter SET count = count + 1 WHERE id = 'coverage_counter'"])

    # Test the generic batch method
    session.batch(:logged, ["INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES ('coverage3', 'Coverage Test 3')"])

    # Test Batch.build
    built_batch = CassandraC::Native::Batch.build(:logged) do |batch|
      stmt = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
      stmt.bind_by_index(0, "coverage4")
      stmt.bind_by_index(1, "Coverage Test 4")
      batch.add(stmt)
    end
    session.execute_batch(built_batch)

    # Test add_all method
    batch = CassandraC::Native::Batch.new(:logged)
    stmt1 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    stmt1.bind_by_index(0, "add_all1")
    stmt1.bind_by_index(1, "Add All 1")
    stmt2 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)", 2)
    stmt2.bind_by_index(0, "add_all2")
    stmt2.bind_by_index(1, "Add All 2")

    batch.add_all([stmt1, stmt2])
    session.execute_batch(batch)
  end

  # Test error case for invalid statement type in generic batch method
  def test_batch_method_with_invalid_statement_type
    assert_raises(ArgumentError) do
      session.batch(:logged, [123])  # Invalid statement type
    end

    assert_raises(ArgumentError) do
      session.batch(:logged, [nil])  # Invalid statement type
    end

    assert_raises(ArgumentError) do
      session.batch(:logged, [{key: "value"}])  # Invalid statement type
    end
  end
end
