# frozen_string_literal: true

require "test_helper"

class TestStatement < Minitest::Test
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

  def test_create_statement
    statement = CassandraC::Native::Statement.new("SELECT * FROM system_schema.tables")
    assert_instance_of CassandraC::Native::Statement, statement
  end

  def test_execute_query
    # Use system_schema to avoid needing to create a keyspace/table
    result = @session.execute("SELECT keyspace_name, table_name FROM system_schema.tables LIMIT 5")
    assert_instance_of CassandraC::Native::Result, result
  end

  def test_result_enumeration
    result = @session.execute("SELECT keyspace_name, table_name FROM system_schema.tables LIMIT 5")
    assert result.row_count > 0
    assert_equal 2, result.column_count

    # Verify column names
    column_names = result.column_names
    assert_equal ["keyspace_name", "table_name"], column_names

    # Verify rows can be enumerated
    row_count = 0
    result.each do |row|
      assert_instance_of Array, row
      assert_equal 2, row.size
      row_count += 1
    end
    assert_equal result.row_count, row_count
  end

  def test_row_values
    result = @session.execute("SELECT keyspace_name, table_name FROM system_schema.tables LIMIT 1")
    row = result.first

    # Test Array access
    assert_instance_of Array, row
    assert_equal 2, row.size
    assert_instance_of String, row[0] # keyspace_name should be a string
    assert_instance_of String, row[1] # table_name should be a string
  end
end
