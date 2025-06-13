# frozen_string_literal: true

require "test_helper"

class TestTimeuuidTypes < Minitest::Test
  def test_timeuuid_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)", 2)
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    statement.bind_by_index(0, "timeuuid_test_1")
    statement.bind_by_index(1, timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'timeuuid_test_1'")
    assert_instance_of CassandraC::Native::TimeUuid, result.to_a.first[0]
    assert_equal timeuuid_str, result.to_a.first[0].to_s
  end

  def test_timeuuid_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (:id, :timeuuid_val)", 2)
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    statement.bind_by_name("id", "timeuuid_test_2")
    statement.bind_by_name("timeuuid_val", timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'timeuuid_test_2'")
    assert_instance_of CassandraC::Native::TimeUuid, result.to_a.first[0]
    assert_equal timeuuid_str, result.to_a.first[0].to_s
  end

  def test_timeuuid_creation_from_string
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    new_timeuuid = CassandraC::Native::TimeUuid.new(timeuuid_str)

    assert_equal timeuuid_str, new_timeuuid.to_s
    assert new_timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_format_validation
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid = CassandraC::Native::TimeUuid.new(timeuuid_str)

    # TimeUUID must be version 1 (indicated by '1' in the 13th position)
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, timeuuid.to_s)
    assert timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_invalid_version
    # Try to create TimeUUID with version 4 UUID (SecureRandom.uuid generates v4)
    uuid_v4 = SecureRandom.uuid
    assert_raises(ArgumentError) do
      CassandraC::Native::TimeUuid.new(uuid_v4)
    end
  end

  def test_timeuuid_equality
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid1 = CassandraC::Native::TimeUuid.new(timeuuid_str)
    timeuuid2 = CassandraC::Native::TimeUuid.new(timeuuid_str)

    assert_equal timeuuid1, timeuuid2
    assert_equal timeuuid1, timeuuid_str
  end

  def test_timeuuid_round_trip
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "timeuuid_round_trip_test")
    statement.bind_by_index(1, timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'timeuuid_round_trip_test'")
    retrieved_timeuuid = result.to_a.first[0]

    assert_instance_of CassandraC::Native::TimeUuid, retrieved_timeuuid
    assert_equal timeuuid_str, retrieved_timeuuid.to_s
  end

  def test_timeuuid_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "timeuuid_null_test")
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'timeuuid_null_test'")
    assert_nil result.to_a.first[0]
  end

  def test_timeuuid_timestamp_extraction
    # Test that TimeUUIDs work in round-trip (timestamp extraction will be implemented in C)
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.uuid_types (id, created_at) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "timeuuid_timestamp_test")
    statement.bind_by_index(1, timeuuid_str, :timeuuid)
    session.execute(statement)

    result = session.query("SELECT created_at FROM cassandra_c_test.uuid_types WHERE id = 'timeuuid_timestamp_test'")
    timeuuid_from_db = result.to_a.first[0]

    assert_instance_of CassandraC::Native::TimeUuid, timeuuid_from_db
    assert_equal timeuuid_str, timeuuid_from_db.to_s
  end

  def test_timeuuid_generation_future
    # TimeUuid.generate will be implemented in C extension later
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid = CassandraC::Native::TimeUuid.new(timeuuid_str)

    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, timeuuid.to_s)
    assert timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_from_time_future
    # TimeUuid.from_time will be implemented in C extension later
    timeuuid_str = "01234567-89ab-1def-8000-123456789abc"
    timeuuid = CassandraC::Native::TimeUuid.new(timeuuid_str)

    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, timeuuid.to_s)
    assert timeuuid.cassandra_typed_timeuuid?
  end
end
