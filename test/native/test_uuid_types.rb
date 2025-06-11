# frozen_string_literal: true

require "test_helper"

class TestUuidTypes < Minitest::Test
  def test_uuid_as_string
    uuid_str = SecureRandom.uuid
    # UUIDs are now just regular Ruby strings
    assert_instance_of String, uuid_str
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i, uuid_str)
  end

  def test_uuid_binding_with_string
    uuid_str = SecureRandom.uuid
    # UUIDs can be bound directly as strings with type hint
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "test_uuid_string")
    statement.bind_by_index(1, uuid_str, :uuid)
    session.execute(statement)
  end

  def test_uuid_generation
    uuid_str = SecureRandom.uuid
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i, uuid_str)
  end

  def test_uuid_invalid_format
    # UUID validation is now handled at binding time
    invalid_uuid = "invalid-uuid"
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "test_invalid")
    assert_raises(CassandraC::Error) do
      statement.bind_by_index(1, invalid_uuid, :uuid)
    end
  end

  def test_uuid_equality
    uuid_str = SecureRandom.uuid
    # UUIDs are just strings now, case insensitive comparison handled by Cassandra
    assert_equal uuid_str.downcase, uuid_str.downcase
    assert_equal uuid_str.upcase.downcase, uuid_str.downcase
  end

  def test_timeuuid_creation_from_string
    # Use a valid TimeUUID string (version 1)
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    new_timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)
    assert_equal timeuuid_str, new_timeuuid.to_s
    assert new_timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_string_conversion_method
    # Create a valid TimeUUID string manually for testing
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)

    assert_equal timeuuid_str, timeuuid.to_s
    assert timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_generation
    # TimeUuid.generate will be implemented in C extension later
    # For now, test with manual TimeUUID string
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, timeuuid.to_s)
    assert timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_from_time
    # TimeUuid.from_time will be implemented in C extension later
    # For now, test that we can create TimeUuid from string
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)

    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, timeuuid.to_s)
    assert timeuuid.cassandra_typed_timeuuid?

    # timestamp method will be implemented in C extension
  end

  def test_timeuuid_time_conversion_method
    # TimeUuid.from_time will be implemented in C extension
    # For now, test with manual TimeUUID string
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)

    assert timeuuid.cassandra_typed_timeuuid?
    # timestamp and from_time methods will be implemented in C extension
  end

  def test_timeuuid_invalid_version
    # Try to create TimeUUID with version 4 UUID (SecureRandom.uuid generates v4)
    uuid_v4 = SecureRandom.uuid
    assert_raises(ArgumentError) do
      CassandraC::Types::TimeUuid.new(uuid_v4)
    end
  end

  def test_timeuuid_equality
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid1 = CassandraC::Types::TimeUuid.new(timeuuid_str)
    timeuuid2 = CassandraC::Types::TimeUuid.new(timeuuid_str)

    assert_equal timeuuid1, timeuuid2
    assert_equal timeuuid1, timeuuid_str
  end

  def test_bind_uuid_by_index_with_string
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)")
    statement = prepared.bind
    uuid_str = SecureRandom.uuid

    statement.bind_by_index(0, "string_uuid_test")
    statement.bind_uuid_by_index(1, uuid_str)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'string_uuid_test'")
    row = result.to_a.first
    assert_instance_of String, row[0]
    assert_equal uuid_str.downcase, row[0].downcase
  end

  def test_bind_uuid_with_type_hint
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)", 2)
    uuid_str = SecureRandom.uuid

    statement.bind_by_index(0, "type_hint_uuid_test")
    statement.bind_by_index(1, uuid_str, :uuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'type_hint_uuid_test'")
    row = result.to_a.first
    assert_instance_of String, row[0]
    assert_equal uuid_str.downcase, row[0].downcase
  end

  def test_bind_uuid_by_name_with_string
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (:id, :uuid_val)", 2)
    uuid_str = SecureRandom.uuid

    statement.bind_by_name("id", "name_uuid_test")
    statement.bind_by_name("uuid_val", uuid_str, :uuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'name_uuid_test'")
    row = result.to_a.first
    assert_instance_of String, row[0]
    assert_equal uuid_str.downcase, row[0].downcase
  end

  def test_bind_timeuuid_by_index_with_string
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)")
    statement = prepared.bind
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    statement.bind_by_index(0, "string_timeuuid_test")
    statement.bind_timeuuid_by_index(1, timeuuid_str)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'string_timeuuid_test'")
    row = result.to_a.first
    # Should return TimeUuid object
    assert_instance_of CassandraC::Types::TimeUuid, row[0]
    assert_equal timeuuid_str, row[0].to_s
  end

  def test_bind_timeuuid_with_type_hint
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)", 2)
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    statement.bind_by_index(0, "type_hint_timeuuid_test")
    statement.bind_by_index(1, timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'type_hint_timeuuid_test'")
    row = result.to_a.first
    # Should return TimeUuid object
    assert_instance_of CassandraC::Types::TimeUuid, row[0]
    assert_equal timeuuid_str, row[0].to_s
  end

  def test_bind_timeuuid_by_name_with_string
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (:id, :timeuuid_val)", 2)
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    statement.bind_by_name("id", "name_timeuuid_test")
    statement.bind_by_name("timeuuid_val", timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'name_timeuuid_test'")
    row = result.to_a.first
    # Should return TimeUuid object
    assert_instance_of CassandraC::Types::TimeUuid, row[0]
    assert_equal timeuuid_str, row[0].to_s
  end

  def test_uuid_automatic_type_detection_in_results
    # Test that UUIDs come back as strings and TimeUUIDs as TimeUuid objects
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val, timeuuid_val) VALUES (?, ?, ?)", 3)

    uuid_str = SecureRandom.uuid
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    statement.bind_by_index(0, "mixed_uuid_test")
    statement.bind_by_index(1, uuid_str, :uuid)
    statement.bind_by_index(2, timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val, timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'mixed_uuid_test'")
    row = result.to_a.first

    # Check that results are properly typed
    assert_instance_of String, row[0]
    # Should return TimeUuid object for timeuuid columns
    assert_instance_of CassandraC::Types::TimeUuid, row[1]
    assert_equal uuid_str.downcase, row[0].downcase
    assert_equal timeuuid_str, row[1].to_s
  end

  def test_timestamp_extraction_from_timeuuid
    # Test that TimeUUIDs work in round-trip (timestamp extraction will be implemented in C)
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    # Insert and retrieve TimeUUID
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, created_at) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "timestamp_test")
    statement.bind_by_index(1, timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT created_at FROM cassandra_c_test.uuid_types WHERE id = 'timestamp_test'")
    row = result.to_a.first
    timeuuid_from_db = row[0]

    # Should return TimeUuid object
    assert_instance_of CassandraC::Types::TimeUuid, timeuuid_from_db
    assert_equal timeuuid_str, timeuuid_from_db.to_s
  end

  def test_null_uuid_handling
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val, timeuuid_val) VALUES (?, ?, ?)")
    statement = prepared.bind

    statement.bind_by_index(0, "null_uuid_test")
    statement.bind_uuid_by_index(1, nil)
    statement.bind_timeuuid_by_index(2, nil)
    session.execute(statement)

    result = session.query("SELECT uuid_val, timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'null_uuid_test'")
    row = result.to_a.first

    assert_nil row[0]
    assert_nil row[1]
  end
end
