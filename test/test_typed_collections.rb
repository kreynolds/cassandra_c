# frozen_string_literal: true

require "test_helper"
require "set"

class TestTypedCollections < Minitest::Test
  def setup
    # Clean up test tables before each test
    session.query("TRUNCATE cassandra_c_test.typed_list_types")
    session.query("TRUNCATE cassandra_c_test.typed_set_types")
    session.query("TRUNCATE cassandra_c_test.typed_map_types")
  end

  # Test type-hinted lists with numeric types
  def test_list_with_tinyint_type_hint
    test_data = [1, 2, 3, 127, -128]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, tinyint_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "tinyint_test")
    statement.bind_list_by_index(1, test_data, :tinyint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT tinyint_list FROM cassandra_c_test.typed_list_types WHERE id = 'tinyint_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal test_data, retrieved_list.map(&:to_i)
  end

  def test_list_with_smallint_type_hint
    test_data = [1000, 2000, 3000, 32767, -32768]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, smallint_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "smallint_test")
    statement.bind_list_by_index(1, test_data, :smallint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT smallint_list FROM cassandra_c_test.typed_list_types WHERE id = 'smallint_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal test_data, retrieved_list.map(&:to_i)
  end

  def test_list_with_int_type_hint
    test_data = [100000, 200000, 300000, 2147483647, -2147483648]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "int_test")
    statement.bind_list_by_index(1, test_data, :int)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_list FROM cassandra_c_test.typed_list_types WHERE id = 'int_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal test_data, retrieved_list.map(&:to_i)
  end

  def test_list_with_bigint_type_hint
    test_data = [1000000000000, 2000000000000, 9223372036854775807, -9223372036854775808]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, bigint_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "bigint_test")
    statement.bind_list_by_index(1, test_data, :bigint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT bigint_list FROM cassandra_c_test.typed_list_types WHERE id = 'bigint_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal test_data, retrieved_list.map(&:to_i)
  end

  def test_list_with_varint_type_hint
    test_data = [123456789012345678901234567890, -987654321098765432109876543210]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, varint_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "varint_test")
    statement.bind_list_by_index(1, test_data, :varint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT varint_list FROM cassandra_c_test.typed_list_types WHERE id = 'varint_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal test_data, retrieved_list.map(&:to_i)
  end

  def test_list_with_float_type_hint
    test_data = [1.5, 2.7, 3.14159, -42.123]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, float_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "float_test")
    statement.bind_list_by_index(1, test_data, :float)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT float_list FROM cassandra_c_test.typed_list_types WHERE id = 'float_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    # Compare with some tolerance for floating point precision
    test_data.each_with_index do |expected, i|
      assert_in_delta expected, retrieved_list[i].to_f, 0.001
    end
  end

  def test_list_with_double_type_hint
    test_data = [1.5123456789, 2.7987654321, 3.141592653589793, -42.123456789012345]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, double_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "double_test")
    statement.bind_list_by_index(1, test_data, :double)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT double_list FROM cassandra_c_test.typed_list_types WHERE id = 'double_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    # Compare with some tolerance for floating point precision
    test_data.each_with_index do |expected, i|
      assert_in_delta expected, retrieved_list[i].to_f, 0.000000000001
    end
  end

  # Test type-hinted sets with numeric types
  def test_set_with_tinyint_type_hint
    test_data = Set.new([1, 2, 3, 127, -128])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_set_types (id, tinyint_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "tinyint_test")
    statement.bind_set_by_index(1, test_data, :tinyint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT tinyint_set FROM cassandra_c_test.typed_set_types WHERE id = 'tinyint_test'")
    retrieved_set = result.to_a.first[0]

    assert_instance_of Set, retrieved_set
    assert_equal test_data, Set.new(retrieved_set.map(&:to_i))
  end

  def test_set_with_bigint_type_hint
    test_data = Set.new([1000000000000, 2000000000000, 9223372036854775807])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_set_types (id, bigint_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "bigint_test")
    statement.bind_set_by_index(1, test_data, :bigint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT bigint_set FROM cassandra_c_test.typed_set_types WHERE id = 'bigint_test'")
    retrieved_set = result.to_a.first[0]

    assert_instance_of Set, retrieved_set
    assert_equal test_data, Set.new(retrieved_set.map(&:to_i))
  end

  # Test type-hinted maps with numeric value types
  def test_map_with_tinyint_value_type_hint
    test_data = {"a" => 1, "b" => 2, "c" => 127, "d" => -128}

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_map_types (id, text_tinyint_map) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "tinyint_test")
    statement.bind_map_by_index(1, test_data, :text, :tinyint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT text_tinyint_map FROM cassandra_c_test.typed_map_types WHERE id = 'tinyint_test'")
    retrieved_map = result.to_a.first[0]

    assert_instance_of Hash, retrieved_map
    test_data.each do |key, expected_value|
      assert_equal expected_value, retrieved_map[key].to_i
    end
  end

  def test_map_with_bigint_value_type_hint
    test_data = {"x" => 1000000000000, "y" => 2000000000000, "z" => 9223372036854775807}

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_map_types (id, text_bigint_map) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "bigint_test")
    statement.bind_map_by_index(1, test_data, :text, :bigint)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT text_bigint_map FROM cassandra_c_test.typed_map_types WHERE id = 'bigint_test'")
    retrieved_map = result.to_a.first[0]

    assert_instance_of Hash, retrieved_map
    test_data.each do |key, expected_value|
      assert_equal expected_value, retrieved_map[key].to_i
    end
  end

  # Test binding by name with type hints
  def test_list_bind_by_name_with_type_hint
    test_data = [100, 200, 300]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, int_list) VALUES (:id, :list)", 2)
    statement.bind_by_name("id", "name_bind_test")
    statement.bind_list_by_name("list", test_data, :int)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_list FROM cassandra_c_test.typed_list_types WHERE id = 'name_bind_test'")
    retrieved_list = result.to_a.first[0]

    assert_equal test_data, retrieved_list.map(&:to_i)
  end

  def test_set_bind_by_name_with_type_hint
    test_data = Set.new([100, 200, 300])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_set_types (id, int_set) VALUES (:id, :int_set)", 2)
    statement.bind_by_name("id", "name_bind_test")
    statement.bind_set_by_name("int_set", test_data, :int)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_set FROM cassandra_c_test.typed_set_types WHERE id = 'name_bind_test'")
    retrieved_set = result.to_a.first[0]

    assert_equal test_data, Set.new(retrieved_set.map(&:to_i))
  end

  def test_map_bind_by_name_with_type_hint
    test_data = {"key1" => 100, "key2" => 200}

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_map_types (id, text_int_map) VALUES (:id, :map)", 2)
    statement.bind_by_name("id", "name_bind_test")
    statement.bind_map_by_name("map", test_data, :text, :int)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT text_int_map FROM cassandra_c_test.typed_map_types WHERE id = 'name_bind_test'")
    retrieved_map = result.to_a.first[0]

    test_data.each do |key, expected_value|
      assert_equal expected_value, retrieved_map[key].to_i
    end
  end

  # Test backward compatibility - methods should work without type hints
  def test_list_backward_compatibility
    test_data = [1, 2, 3]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "compat_test")
    statement.bind_list_by_index(1, test_data)  # No type hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_list FROM cassandra_c_test.typed_list_types WHERE id = 'compat_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal test_data, retrieved_list.map(&:to_i)
  end

  def test_set_backward_compatibility
    test_data = Set.new([1, 2, 3])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_set_types (id, int_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "compat_test")
    statement.bind_set_by_index(1, test_data)  # No type hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_set FROM cassandra_c_test.typed_set_types WHERE id = 'compat_test'")
    retrieved_set = result.to_a.first[0]

    assert_instance_of Set, retrieved_set
    assert_equal test_data, Set.new(retrieved_set.map(&:to_i))
  end

  def test_map_backward_compatibility
    test_data = {"key1" => 100, "key2" => 200}

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_map_types (id, text_int_map) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "compat_test")
    statement.bind_map_by_index(1, test_data)  # No type hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT text_int_map FROM cassandra_c_test.typed_map_types WHERE id = 'compat_test'")
    retrieved_map = result.to_a.first[0]

    test_data.each do |key, expected_value|
      assert_equal expected_value, retrieved_map[key].to_i
    end
  end

  # Test edge cases
  def test_empty_collections_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "empty_test")
    statement.bind_list_by_index(1, [], :int)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_list FROM cassandra_c_test.typed_list_types WHERE id = 'empty_test'")
    retrieved_list = result.to_a.first[0]

    # Empty collections might be returned as nil or empty array
    assert(retrieved_list.nil? || retrieved_list.empty?)
  end

  def test_null_collections_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "null_test")
    statement.bind_list_by_index(1, nil, :int)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_list FROM cassandra_c_test.typed_list_types WHERE id = 'null_test'")
    retrieved_list = result.to_a.first[0]

    assert_nil retrieved_list
  end

  # Test with mixed numeric types - should work with proper type hints
  def test_mixed_numeric_types_in_list
    # Mix different Ruby numeric types but hint as int
    test_data = [1, 2.0, 3]  # Mix Fixnum and Float
    expected_data = [1, 2, 3]  # All should be converted to ints

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "mixed_test")
    statement.bind_list_by_index(1, test_data, :int)

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_list FROM cassandra_c_test.typed_list_types WHERE id = 'mixed_test'")
    retrieved_list = result.to_a.first[0]

    assert_equal expected_data, retrieved_list.map(&:to_i)
  end
end
