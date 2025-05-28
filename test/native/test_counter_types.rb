# frozen_string_literal: true

require "test_helper"

class TestCounterTypes < Minitest::Test
  def setup
    # Clean up counter tables before each test - counters persist values
    session.query("TRUNCATE cassandra_c_test.single_counter")
    session.query("TRUNCATE cassandra_c_test.counter_table")
  end

  def test_counter_type_identification
    # Counters should map to BigInt type since they're 64-bit integers
    counter_val = 42.to_cassandra_bigint
    assert_instance_of CassandraC::Types::BigInt, counter_val
    assert_equal(42, counter_val.to_i)

    # Test large counter values
    large_counter = 9223372036854775807.to_cassandra_bigint  # Max int64
    assert_instance_of CassandraC::Types::BigInt, large_counter
    assert_equal(9223372036854775807, large_counter.to_i)
  end

  def test_counter_increment_operations
    # Initialize counter with UPDATE statement
    session.query("UPDATE cassandra_c_test.single_counter SET count = count + 10 WHERE id = 'test1'")

    # Verify initial value
    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test1'")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    assert_instance_of CassandraC::Types::BigInt, row[0]
    assert_equal(10, row[0].to_i)

    # Increment by 5
    session.query("UPDATE cassandra_c_test.single_counter SET count = count + 5 WHERE id = 'test1'")

    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test1'")
    row = result.to_a.first
    assert_equal(15, row[0].to_i)
  end

  def test_counter_decrement_operations
    # Initialize and then decrement
    session.query("UPDATE cassandra_c_test.single_counter SET count = count + 100 WHERE id = 'test2'")
    session.query("UPDATE cassandra_c_test.single_counter SET count = count - 25 WHERE id = 'test2'")

    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test2'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::BigInt, row[0]
    assert_equal(75, row[0].to_i)
  end

  def test_multiple_counter_columns
    # Test table with multiple counter columns
    session.query("UPDATE cassandra_c_test.counter_table SET page_views = page_views + 100, unique_visitors = unique_visitors + 10 WHERE id = 'page1' AND category = 'tech'")

    result = session.query("SELECT page_views, unique_visitors FROM cassandra_c_test.counter_table WHERE id = 'page1' AND category = 'tech'")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    assert_instance_of CassandraC::Types::BigInt, row[0]  # page_views
    assert_instance_of CassandraC::Types::BigInt, row[1]  # unique_visitors
    assert_equal(100, row[0].to_i)
    assert_equal(10, row[1].to_i)

    # Increment counters again
    session.query("UPDATE cassandra_c_test.counter_table SET page_views = page_views + 50, unique_visitors = unique_visitors + 5 WHERE id = 'page1' AND category = 'tech'")

    result = session.query("SELECT page_views, unique_visitors FROM cassandra_c_test.counter_table WHERE id = 'page1' AND category = 'tech'")
    row = result.to_a.first
    assert_equal(150, row[0].to_i)
    assert_equal(15, row[1].to_i)
  end

  def test_prepared_statement_with_counter_updates
    # Test prepared statements with counter updates
    prepared = session.prepare("UPDATE cassandra_c_test.single_counter SET count = count + ? WHERE id = ?")

    increment_val = 42.to_cassandra_bigint
    statement = prepared.bind([increment_val, "test3"])
    session.execute(statement)

    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test3'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::BigInt, row[0]
    assert_equal(42, row[0].to_i)

    # Test negative increment (decrement)
    decrement_val = -10.to_cassandra_bigint
    statement = prepared.bind([decrement_val, "test3"])
    session.execute(statement)

    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test3'")
    row = result.to_a.first
    assert_equal(32, row[0].to_i)
  end

  def test_counter_arithmetic_preservation
    # Test that counter values preserve BigInt type through arithmetic
    counter1 = 100.to_cassandra_bigint
    counter2 = 50.to_cassandra_bigint

    result = counter1 + counter2
    assert_instance_of CassandraC::Types::BigInt, result
    assert_equal(150, result.to_i)

    result = counter1 - counter2
    assert_instance_of CassandraC::Types::BigInt, result
    assert_equal(50, result.to_i)

    result = counter1 * 2
    assert_instance_of CassandraC::Types::BigInt, result
    assert_equal(200, result.to_i)
  end

  def test_large_counter_values
    # Test with very large counter values (near int64 limits)
    large_val = 9223372036854775000.to_cassandra_bigint

    session.query("UPDATE cassandra_c_test.single_counter SET count = count + #{large_val.to_i} WHERE id = 'test_large'")

    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test_large'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::BigInt, row[0]
    assert_equal(9223372036854775000, row[0].to_i)
  end

  def test_counter_edge_cases
    # Test zero counter values
    session.query("UPDATE cassandra_c_test.single_counter SET count = count + 0 WHERE id = 'test_zero'")

    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test_zero'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::BigInt, row[0]
    assert_equal(0, row[0].to_i)

    # Test negative counter values
    session.query("UPDATE cassandra_c_test.single_counter SET count = count - 100 WHERE id = 'test_zero'")

    result = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'test_zero'")
    row = result.to_a.first
    assert_equal(-100, row[0].to_i)
  end

  def test_counter_batch_operations
    # Test batch statements with counter updates (note: counter batches have special requirements)
    session.query("BEGIN COUNTER BATCH UPDATE cassandra_c_test.single_counter SET count = count + 1 WHERE id = 'batch1'; UPDATE cassandra_c_test.single_counter SET count = count + 2 WHERE id = 'batch2'; APPLY BATCH")

    result1 = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'batch1'")
    result2 = session.query("SELECT count FROM cassandra_c_test.single_counter WHERE id = 'batch2'")

    row1 = result1.to_a.first
    row2 = result2.to_a.first

    assert_instance_of CassandraC::Types::BigInt, row1[0]
    assert_instance_of CassandraC::Types::BigInt, row2[0]
    assert_equal(1, row1[0].to_i)
    assert_equal(2, row2[0].to_i)
  end
end
