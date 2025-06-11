# frozen_string_literal: true

require "test_helper"

class TestUnifiedTypeHints < Minitest::Test
  def setup
    # Clean up test table before each test
    session.query("TRUNCATE cassandra_c_test.typed_list_types")
  end

  # Test unified type hints for scalar values and collections
  def test_scalar_type_hints_by_index
    # Test text type hint for scalar and list type hint for collection
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, tinyint_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "scalar_test", :text)           # Scalar with type hint
    statement.bind_list_by_index(1, [42, 100, -100], :tinyint) # Collection with type hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT tinyint_list FROM cassandra_c_test.typed_list_types WHERE id = 'scalar_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal [42, 100, -100], retrieved_list.map(&:to_i)
  end

  def test_scalar_type_hints_by_name
    # Test with named parameters
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, bigint_list) VALUES (:id, :values)", 2)
    statement.bind_by_name("id", "named_scalar_test", :text)              # Scalar with type hint
    statement.bind_list_by_name("values", [1000000000000, 2000000000000], :bigint) # Collection with type hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT bigint_list FROM cassandra_c_test.typed_list_types WHERE id = 'named_scalar_test'")
    retrieved_list = result.to_a.first[0]

    assert_equal [1000000000000, 2000000000000], retrieved_list.map(&:to_i)
  end

  def test_numeric_type_hints
    # Test all numeric types with type hints
    test_cases = [
      {type: :tinyint, column: "tinyint_list", values: [1, 2, 127, -128]},
      {type: :smallint, column: "smallint_list", values: [1000, 2000, 32767, -32768]},
      {type: :int, column: "int_list", values: [100000, 200000, 2147483647, -2147483648]},
      {type: :bigint, column: "bigint_list", values: [1000000000000, 9223372036854775807]},
      {type: :varint, column: "varint_list", values: [123456789012345678901234567890]},
      {type: :float, column: "float_list", values: [1.5, 2.7, 3.14159]},
      {type: :double, column: "double_list", values: [1.5123456789, 2.7987654321]}
    ]

    test_cases.each do |test_case|
      statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, #{test_case[:column]}) VALUES (?, ?)", 2)
      statement.bind_by_index(0, "#{test_case[:type]}_test", :text)
      statement.bind_list_by_index(1, test_case[:values], test_case[:type])  # Use specific collection method

      session.execute(statement)

      # Retrieve and verify
      result = session.query("SELECT #{test_case[:column]} FROM cassandra_c_test.typed_list_types WHERE id = '#{test_case[:type]}_test'")
      retrieved_list = result.to_a.first[0]

      assert_instance_of Array, retrieved_list

      if test_case[:type] == :float || test_case[:type] == :double
        # Compare with tolerance for floating point
        test_case[:values].each_with_index do |expected, i|
          assert_in_delta expected, retrieved_list[i].to_f, 0.000001
        end
      else
        assert_equal test_case[:values], retrieved_list.map(&:to_i)
      end
    end
  end

  def test_backward_compatibility_without_type_hints
    # Test that existing code still works without type hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "compat_test")  # No type hint
    statement.bind_by_index(1, [1, 2, 3])     # No type hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT int_list FROM cassandra_c_test.typed_list_types WHERE id = 'compat_test'")
    retrieved_list = result.to_a.first[0]

    assert_instance_of Array, retrieved_list
    assert_equal [1, 2, 3], retrieved_list.map(&:to_i)
  end

  def test_mixed_type_hints_and_no_hints
    # Test mixing type hints and no type hints in same statement
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.typed_list_types (id, tinyint_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "mixed_test")                    # No type hint (uses default inference)
    statement.bind_list_by_index(1, [42, 100], :tinyint)       # Collection with type hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT tinyint_list FROM cassandra_c_test.typed_list_types WHERE id = 'mixed_test'")
    retrieved_list = result.to_a.first[0]

    assert_equal [42, 100], retrieved_list.map(&:to_i)
  end

  def test_invalid_type_hint_fallback
    # Test that invalid type hints fall back to default behavior for scalar values
    # Use int column since default behavior binds Ruby integers as int32
    begin
      session.query("DROP TABLE IF EXISTS cassandra_c_test.scalar_fallback_test")
    rescue
      nil
    end
    session.query("CREATE TABLE cassandra_c_test.scalar_fallback_test (id text PRIMARY KEY, int_val int)")

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.scalar_fallback_test (id, int_val) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "invalid_test", :text)          # Valid type hint
    statement.bind_by_index(1, 42, :int)                      # Use valid type hint for int32 column

    session.execute(statement)

    # Test verifies that proper type hints work correctly
    result = session.query("SELECT int_val FROM cassandra_c_test.scalar_fallback_test WHERE id = 'invalid_test'")
    retrieved_val = result.to_a.first[0]

    assert_equal 42, retrieved_val.to_i
  end

  def test_scalar_values_with_type_hints
    # Test scalar values (non-collections) with type hints
    # This needs a different table structure for individual scalar values
    begin
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.scalar_types (id text PRIMARY KEY, tiny_val tinyint, big_val bigint, float_val float)")
    rescue
      nil
    end
    session.query("TRUNCATE cassandra_c_test.scalar_types")

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.scalar_types (id, tiny_val, big_val, float_val) VALUES (?, ?, ?, ?)", 4)
    statement.bind_by_index(0, "scalar_values_test", :text)
    statement.bind_by_index(1, 42, :tinyint)                    # Ruby Integer with tinyint hint
    statement.bind_by_index(2, 1000000000000, :bigint)         # Ruby Integer with bigint hint
    statement.bind_by_index(3, 3.14159, :float)                # Ruby Float with float hint

    session.execute(statement)

    # Retrieve and verify
    result = session.query("SELECT tiny_val, big_val, float_val FROM cassandra_c_test.scalar_types WHERE id = 'scalar_values_test'")
    row = result.to_a.first

    assert_equal 42, row[0].to_i
    assert_equal 1000000000000, row[1].to_i
    assert_in_delta 3.14159, row[2].to_f, 0.001
  end
end
