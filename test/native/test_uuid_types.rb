# frozen_string_literal: true

require "test_helper"

class TestUuidTypes < Minitest::Test
  def test_uuid_creation_from_string
    uuid_str = SecureRandom.uuid
    uuid = CassandraC::Types::Uuid.new(uuid_str)
    assert_equal uuid_str.downcase, uuid.to_s
    assert uuid.cassandra_typed_uuid?
  end

  def test_uuid_string_conversion_method
    uuid_str = SecureRandom.uuid
    uuid = uuid_str.to_cassandra_uuid
    assert_equal uuid_str.downcase, uuid.to_s
    assert uuid.cassandra_typed_uuid?
  end

  def test_uuid_generation
    uuid = CassandraC::Types::Uuid.generate
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/, uuid.to_s)
    assert uuid.cassandra_typed_uuid?
  end

  def test_uuid_invalid_format
    assert_raises(ArgumentError) do
      CassandraC::Types::Uuid.new("invalid-uuid")
    end
  end

  def test_uuid_equality
    uuid_str = SecureRandom.uuid
    uuid1 = CassandraC::Types::Uuid.new(uuid_str)
    uuid2 = CassandraC::Types::Uuid.new(uuid_str.upcase)

    assert_equal uuid1, uuid2
    assert_equal uuid1, uuid_str
    assert_equal uuid1, uuid_str.upcase
  end

  def test_timeuuid_creation_from_string
    # Generate a valid TimeUUID (version 1)
    timeuuid = CassandraC::Types::TimeUuid.generate
    timeuuid_str = timeuuid.to_s

    new_timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)
    assert_equal timeuuid_str, new_timeuuid.to_s
    assert new_timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_string_conversion_method
    timeuuid = CassandraC::Types::TimeUuid.generate
    timeuuid_str = timeuuid.to_s

    converted = timeuuid_str.to_cassandra_timeuuid
    assert_equal timeuuid_str, converted.to_s
    assert converted.cassandra_typed_timeuuid?
  end

  def test_timeuuid_generation
    timeuuid = CassandraC::Types::TimeUuid.generate
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, timeuuid.to_s)
    assert timeuuid.cassandra_typed_timeuuid?
  end

  def test_timeuuid_from_time
    timestamp = Time.now - rand(86400) # Random time within last 24 hours
    timeuuid = CassandraC::Types::TimeUuid.from_time(timestamp)

    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, timeuuid.to_s)
    assert timeuuid.cassandra_typed_timeuuid?

    # Extract timestamp and verify it's close to original
    extracted_time = timeuuid.timestamp
    assert_in_delta timestamp.to_f, extracted_time.to_f, 0.01
  end

  def test_timeuuid_time_conversion_method
    timestamp = Time.now
    timeuuid = timestamp.to_cassandra_timeuuid

    assert timeuuid.cassandra_typed_timeuuid?
    extracted_time = timeuuid.timestamp
    assert_in_delta timestamp.to_f, extracted_time.to_f, 0.01
  end

  def test_timeuuid_invalid_version
    # Try to create TimeUUID with version 4 UUID (SecureRandom.uuid generates v4)
    uuid_v4 = SecureRandom.uuid
    assert_raises(ArgumentError) do
      CassandraC::Types::TimeUuid.new(uuid_v4)
    end
  end

  def test_timeuuid_equality
    timestamp = Time.now - rand(86400)
    timeuuid1 = CassandraC::Types::TimeUuid.from_time(timestamp)
    timeuuid2 = CassandraC::Types::TimeUuid.new(timeuuid1.to_s)

    assert_equal timeuuid1, timeuuid2
    assert_equal timeuuid1, timeuuid1.to_s
  end

  def test_bind_uuid_by_index_with_typed_uuid
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)")
    statement = prepared.bind
    uuid = CassandraC::Types::Uuid.generate

    statement.bind_by_index(0, "typed_uuid_test")
    statement.bind_uuid_by_index(1, uuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'typed_uuid_test'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::Uuid, row[0]
    assert_equal uuid.to_s, row[0].to_s
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
    assert_instance_of CassandraC::Types::Uuid, row[0]
    assert_equal uuid_str.downcase, row[0].to_s
  end

  def test_bind_uuid_by_name_with_typed_uuid
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val) VALUES (?, ?)")
    statement = prepared.bind
    uuid = CassandraC::Types::Uuid.generate

    statement.bind_by_index(0, "typed_uuid_name_test")
    statement.bind_uuid_by_name("uuid_val", uuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val FROM cassandra_c_test.uuid_types WHERE id = 'typed_uuid_name_test'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::Uuid, row[0]
    assert_equal uuid.to_s, row[0].to_s
  end

  def test_bind_timeuuid_by_index_with_typed_timeuuid
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)")
    statement = prepared.bind
    timeuuid = CassandraC::Types::TimeUuid.generate

    statement.bind_by_index(0, "typed_timeuuid_test")
    statement.bind_timeuuid_by_index(1, timeuuid)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'typed_timeuuid_test'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::TimeUuid, row[0]
    assert_equal timeuuid.to_s, row[0].to_s
  end

  def test_bind_timeuuid_by_index_with_string
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)")
    statement = prepared.bind
    timeuuid = CassandraC::Types::TimeUuid.generate
    timeuuid_str = timeuuid.to_s

    statement.bind_by_index(0, "string_timeuuid_test")
    statement.bind_timeuuid_by_index(1, timeuuid_str)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'string_timeuuid_test'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::TimeUuid, row[0]
    assert_equal timeuuid_str, row[0].to_s
  end

  def test_bind_timeuuid_by_name_with_typed_timeuuid
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, timeuuid_val) VALUES (?, ?)")
    statement = prepared.bind
    timeuuid = CassandraC::Types::TimeUuid.generate

    statement.bind_by_index(0, "typed_timeuuid_name_test")
    statement.bind_timeuuid_by_name("timeuuid_val", timeuuid)
    session.execute(statement)

    result = session.query("SELECT timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'typed_timeuuid_name_test'")
    row = result.to_a.first
    assert_instance_of CassandraC::Types::TimeUuid, row[0]
    assert_equal timeuuid.to_s, row[0].to_s
  end

  def test_uuid_automatic_type_detection_in_results
    # Test that UUIDs and TimeUUIDs are automatically converted to correct types from results
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, uuid_val, timeuuid_val) VALUES (?, ?, ?)")
    statement = prepared.bind

    uuid = CassandraC::Types::Uuid.generate
    timeuuid = CassandraC::Types::TimeUuid.generate

    statement.bind_by_index(0, "mixed_uuid_test")
    statement.bind_by_index(1, uuid)
    statement.bind_by_index(2, timeuuid)
    session.execute(statement)

    result = session.query("SELECT uuid_val, timeuuid_val FROM cassandra_c_test.uuid_types WHERE id = 'mixed_uuid_test'")
    row = result.to_a.first

    # Check that results are properly typed
    assert_instance_of CassandraC::Types::Uuid, row[0]
    assert_instance_of CassandraC::Types::TimeUuid, row[1]
    assert_equal uuid.to_s, row[0].to_s
    assert_equal timeuuid.to_s, row[1].to_s
  end

  def test_timestamp_extraction_from_timeuuid
    # Test that TimeUUIDs correctly extract timestamps in round-trip
    timestamp = Time.now - rand(86400)
    timeuuid = CassandraC::Types::TimeUuid.from_time(timestamp)

    # Insert and retrieve TimeUUID
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, created_at) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "timestamp_test")
    statement.bind_by_index(1, timeuuid)
    session.execute(statement)

    result = session.query("SELECT created_at FROM cassandra_c_test.uuid_types WHERE id = 'timestamp_test'")
    row = result.to_a.first
    timeuuid_from_db = row[0]

    # Verify timestamp extraction works correctly
    assert_instance_of CassandraC::Types::TimeUuid, timeuuid_from_db
    assert_in_delta timestamp.to_f, timeuuid_from_db.timestamp.to_f, 0.1
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
