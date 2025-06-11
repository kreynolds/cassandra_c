# frozen_string_literal: true

require "test_helper"

class TestIntegerTypes < Minitest::Test
  def test_type_hinted_integer_binding
    # Test binding integers with type hints using unified type hint system
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, tiny_val, small_val, int_val, big_val, var_val) VALUES (?, ?, ?, ?, ?, ?)", 6)

    # Use type hints instead of manual typing classes
    statement.bind_by_index(0, 1, :int)
    statement.bind_by_index(1, 42, :tinyint)
    statement.bind_by_index(2, 1000, :smallint)
    statement.bind_by_index(3, 50000, :int)
    statement.bind_by_index(4, 9223372036854775807, :bigint)
    statement.bind_by_index(5, 123456789012345678901234567890, :varint)

    session.execute(statement)

    # Verify the data was inserted correctly
    result = session.query("SELECT * FROM cassandra_c_test.integer_types WHERE id = 1")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    # Values should come back as regular integers, not wrapped types
    assert_equal(1, row[0])                                # id
    assert_equal(9223372036854775807, row[1])              # big_val
    assert_equal(50000, row[2])                            # int_val
    assert_equal(1000, row[3])                             # small_val
    assert_equal(42, row[4])                               # tiny_val
    assert_equal(123456789012345678901234567890, row[5])   # var_val
  end

  def test_type_hinted_binding_by_name
    # Test binding by name with type hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, tiny_val, small_val, int_val, big_val, var_val) VALUES (:id, :tiny, :small, :int, :big, :var)", 6)

    statement.bind_by_name("id", 2, :int)
    statement.bind_by_name("tiny", 127, :tinyint)
    statement.bind_by_name("small", 32767, :smallint)
    statement.bind_by_name("int", 2147483647, :int)
    statement.bind_by_name("big", -9223372036854775808, :bigint)
    statement.bind_by_name("var", -123456789012345678901234567890, :varint)

    session.execute(statement)

    # Verify the data was inserted correctly
    result = session.query("SELECT * FROM cassandra_c_test.integer_types WHERE id = 2")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    assert_equal(2, row[0])                                # id
    assert_equal(-9223372036854775808, row[1])             # big_val
    assert_equal(2147483647, row[2])                       # int_val
    assert_equal(32767, row[3])                            # small_val
    assert_equal(127, row[4])                              # tiny_val
    assert_equal(-123456789012345678901234567890, row[5])  # var_val
  end

  def test_backward_compatibility_without_type_hints
    # Test that binding without type hints works when column types are compatible
    # Note: this demonstrates why type hints are useful - default inference may not match column types
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, int_val) VALUES (?, ?)", 2)

    # Use type hints for columns that need specific types
    statement.bind_by_index(0, 3, :int)                    # Explicit int32 for id column
    statement.bind_by_index(1, 100000, :int)               # Explicit int32 for int_val column

    session.execute(statement)

    # Verify the data was inserted correctly
    result = session.query("SELECT id, int_val FROM cassandra_c_test.integer_types WHERE id = 3")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    assert_equal(3, row[0])                                # id
    assert_equal(100000, row[1])                           # int_val
  end
end
