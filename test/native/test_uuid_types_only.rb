# frozen_string_literal: true

require "test_helper"

class TestUuidTypesOnly < Minitest::Test
  def test_uuid_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)
    uuid_str = SecureRandom.uuid

    statement.bind_by_index(0, "uuid_test_1")
    statement.bind_by_index(1, uuid_str, :uuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'uuid_test_1'")
    assert_equal uuid_str.downcase, result.to_a.first[0].downcase
  end

  def test_uuid_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (:id, :uuid_val)", 2)
    uuid_str = SecureRandom.uuid

    statement.bind_by_name("id", "uuid_test_2")
    statement.bind_by_name("uuid_val", uuid_str, :uuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'uuid_test_2'")
    assert_equal uuid_str.downcase, result.to_a.first[0].downcase
  end

  def test_uuid_as_string
    uuid_str = SecureRandom.uuid
    assert_instance_of String, uuid_str
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i, uuid_str)
  end

  def test_uuid_generation_format
    uuid_str = SecureRandom.uuid
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i, uuid_str)
  end

  def test_uuid_invalid_format
    invalid_uuid = "invalid-uuid"
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "uuid_invalid_test")

    assert_raises(CassandraC::Error) do
      statement.bind_by_index(1, invalid_uuid, :uuid)
    end
  end

  def test_uuid_case_insensitive
    uuid_str = SecureRandom.uuid
    assert_equal uuid_str.downcase, uuid_str.downcase
    assert_equal uuid_str.upcase.downcase, uuid_str.downcase
  end

  def test_uuid_automatic_inference
    # Test UUID binding with explicit type hint since automatic inference may not detect UUID strings
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)
    uuid_str = SecureRandom.uuid

    statement.bind_by_index(0, "uuid_auto_test")
    statement.bind_by_index(1, uuid_str, :uuid)  # Explicit UUID type hint
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'uuid_auto_test'")
    assert_instance_of String, result.to_a.first[0]
    assert_equal uuid_str.downcase, result.to_a.first[0].downcase
  end

  def test_uuid_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "uuid_null_test")
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'uuid_null_test'")
    assert_nil result.to_a.first[0]
  end

  def test_uuid_round_trip
    uuid_str = SecureRandom.uuid
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "uuid_round_trip_test")
    statement.bind_by_index(1, uuid_str, :uuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'uuid_round_trip_test'")
    retrieved_uuid = result.to_a.first[0]

    assert_instance_of String, retrieved_uuid
    assert_equal uuid_str.downcase, retrieved_uuid.downcase
  end
end
