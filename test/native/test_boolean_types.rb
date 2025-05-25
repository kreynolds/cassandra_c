# frozen_string_literal: true

require "test_helper"

class TestBooleanTypes < Minitest::Test
  def setup
    @cluster = CassandraC::Native::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }

    @session = CassandraC::Native::Session.new
    @session.connect(@cluster)
  end

  def teardown
    @session&.close
  end

  def test_boolean_binding_with_array_parameters
    # Test binding boolean values using array parameter binding
    prepared = @session.prepare("INSERT INTO cassandra_c_test.boolean_test (id, bool_val, nullable_bool) VALUES (?, ?, ?)")

    # Test true value
    statement = prepared.bind([1, true, true])
    @session.execute(statement)

    # Test false value
    statement = prepared.bind([2, false, false])
    @session.execute(statement)

    # Test nil (null) value
    statement = prepared.bind([3, true, nil])
    @session.execute(statement)

    # Verify data was inserted correctly
    result = @session.query("SELECT * FROM cassandra_c_test.boolean_test WHERE id IN (1, 2, 3)")
    rows = result.to_a.sort_by { |row| row[0].to_i }
    assert_equal(3, rows.length)

    # Row 1: true, true
    assert_equal(1, rows[0][0].to_i)
    assert_equal(true, rows[0][1])
    assert_equal(true, rows[0][2])

    # Row 2: false, false
    assert_equal(2, rows[1][0].to_i)
    assert_equal(false, rows[1][1])
    assert_equal(false, rows[1][2])

    # Row 3: true, nil
    assert_equal(3, rows[2][0].to_i)
    assert_equal(true, rows[2][1])
    assert_nil(rows[2][2])
  end

  def test_boolean_binding_by_index
    # Test binding boolean values by index
    prepared = @session.prepare("INSERT INTO cassandra_c_test.boolean_bind_test (id, value) VALUES (?, ?)")

    # Test binding true by index
    statement = prepared.bind
    statement.bind_by_index(0, "test_true")
    statement.bind_by_index(1, true)
    @session.execute(statement)

    # Test binding false by index
    statement = prepared.bind
    statement.bind_by_index(0, "test_false")
    statement.bind_by_index(1, false)
    @session.execute(statement)

    # Test binding nil by index
    statement = prepared.bind
    statement.bind_by_index(0, "test_nil")
    statement.bind_by_index(1, nil)
    @session.execute(statement)

    # Verify data
    result = @session.query("SELECT * FROM cassandra_c_test.boolean_bind_test WHERE id IN ('test_true', 'test_false', 'test_nil')")
    rows = result.to_a.sort_by { |row| row[0] }
    assert_equal(3, rows.length)

    # test_false row
    assert_equal("test_false", rows[0][0])
    assert_equal(false, rows[0][1])

    # test_nil row
    assert_equal("test_nil", rows[1][0])
    assert_nil(rows[1][1])

    # test_true row
    assert_equal("test_true", rows[2][0])
    assert_equal(true, rows[2][1])
  end

  def test_boolean_binding_by_name
    # Test binding boolean values by name
    prepared = @session.prepare("INSERT INTO cassandra_c_test.boolean_bind_test (id, value) VALUES (:id, :value)")

    # Test binding true by name
    statement = prepared.bind
    statement.bind_by_name("id", "named_true")
    statement.bind_by_name("value", true)
    @session.execute(statement)

    # Test binding false by name
    statement = prepared.bind
    statement.bind_by_name("id", "named_false")
    statement.bind_by_name("value", false)
    @session.execute(statement)

    # Test binding nil by name
    statement = prepared.bind
    statement.bind_by_name("id", "named_nil")
    statement.bind_by_name("value", nil)
    @session.execute(statement)

    # Verify data
    result = @session.query("SELECT * FROM cassandra_c_test.boolean_bind_test WHERE id IN ('named_true', 'named_false', 'named_nil')")
    rows = result.to_a.sort_by { |row| row[0] }
    assert_equal(3, rows.length)

    # named_false row
    assert_equal("named_false", rows[0][0])
    assert_equal(false, rows[0][1])

    # named_nil row
    assert_equal("named_nil", rows[1][0])
    assert_nil(rows[1][1])

    # named_true row
    assert_equal("named_true", rows[2][0])
    assert_equal(true, rows[2][1])
  end

  def test_boolean_result_types
    # Insert test data
    @session.query("INSERT INTO cassandra_c_test.boolean_test (id, bool_val, nullable_bool) VALUES (10, true, false)")
    @session.query("INSERT INTO cassandra_c_test.boolean_test (id, bool_val, nullable_bool) VALUES (11, false, true)")
    @session.query("INSERT INTO cassandra_c_test.boolean_test (id, bool_val, nullable_bool) VALUES (12, true, null)")

    # Query and verify result types
    result = @session.query("SELECT * FROM cassandra_c_test.boolean_test WHERE id IN (10, 11, 12)")
    rows = result.to_a.sort_by { |row| row[0].to_i }
    assert_equal(3, rows.length)

    # Verify that boolean results are returned as Ruby true/false/nil
    rows.each do |row|
      # Column 1 and 2 should be boolean or nil values
      assert([true, false].include?(row[1]), "Column 1 should be true or false, got #{row[1].class}: #{row[1]}")
      assert([true, false, nil].include?(row[2]), "Column 2 should be true, false, or nil, got #{row[2].class}: #{row[2]}")
    end

    # Row 10: true, false
    assert_equal(true, rows[0][1])
    assert_equal(false, rows[0][2])

    # Row 11: false, true
    assert_equal(false, rows[1][1])
    assert_equal(true, rows[1][2])

    # Row 12: true, nil
    assert_equal(true, rows[2][1])
    assert_nil(rows[2][2])
  end

  def test_boolean_with_simple_queries
    # Test boolean values in simple (non-prepared) queries
    @session.query("INSERT INTO cassandra_c_test.boolean_test (id, bool_val, nullable_bool) VALUES (20, true, false)")
    @session.query("INSERT INTO cassandra_c_test.boolean_test (id, bool_val, nullable_bool) VALUES (21, false, true)")

    # Query by primary key and verify boolean values
    result = @session.query("SELECT * FROM cassandra_c_test.boolean_test WHERE id = 20")
    rows = result.to_a
    assert_equal(1, rows.length)
    assert_equal(20, rows[0][0].to_i)
    assert_equal(true, rows[0][1])
    assert_equal(false, rows[0][2])

    result = @session.query("SELECT * FROM cassandra_c_test.boolean_test WHERE id = 21")
    rows = result.to_a
    assert_equal(1, rows.length)
    assert_equal(21, rows[0][0].to_i)
    assert_equal(false, rows[0][1])
    assert_equal(true, rows[0][2])
  end

  def test_boolean_edge_cases
    # Test that Ruby truthy/falsy values are handled correctly
    prepared = @session.prepare("INSERT INTO cassandra_c_test.boolean_bind_test (id, value) VALUES (?, ?)")

    # Only true and false should be accepted as booleans, not truthy/falsy values
    statement = prepared.bind(["edge_true", true])
    @session.execute(statement)

    statement = prepared.bind(["edge_false", false])
    @session.execute(statement)

    # Verify the data
    result = @session.query("SELECT * FROM cassandra_c_test.boolean_bind_test WHERE id IN ('edge_true', 'edge_false')")
    rows = result.to_a.sort_by { |row| row[0] }
    assert_equal(2, rows.length)

    assert_equal("edge_false", rows[0][0])
    assert_equal(false, rows[0][1])

    assert_equal("edge_true", rows[1][0])
    assert_equal(true, rows[1][1])
  end
end
