# frozen_string_literal: true

require "test_helper"

class TestUuidTypes < Minitest::Test
  def test_uuid_creation_from_string
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    uuid = CassandraC::Types::Uuid.new(uuid_str)
    assert_equal uuid_str.downcase, uuid.to_s
    assert uuid.cassandra_typed_uuid?
  end

  def test_uuid_string_conversion_method
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
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
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
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
    timestamp = Time.new(2023, 6, 15, 12, 30, 45)
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
    # Try to create TimeUUID with version 4 UUID
    uuid_v4 = "550e8400-e29b-41d4-a716-446655440000"
    assert_raises(ArgumentError) do
      CassandraC::Types::TimeUuid.new(uuid_v4)
    end
  end

  def test_timeuuid_equality
    timestamp = Time.new(2023, 6, 15, 12, 30, 45)
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
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"

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
    # Test that TimeUUIDs correctly extract timestamps
    time1 = Time.new(2023, 1, 1, 12, 0, 0)
    time2 = Time.new(2023, 6, 1, 12, 0, 0)
    time3 = Time.new(2023, 12, 1, 12, 0, 0)

    timeuuid1 = CassandraC::Types::TimeUuid.from_time(time1)
    timeuuid2 = CassandraC::Types::TimeUuid.from_time(time2)
    timeuuid3 = CassandraC::Types::TimeUuid.from_time(time3)

    # Insert TimeUUIDs
    prepared = session.prepare("INSERT INTO cassandra_c_test.uuid_types (id, created_at) VALUES (?, ?)")

    ["event1", "event2", "event3"].zip([timeuuid1, timeuuid2, timeuuid3]).each do |id, tu|
      statement = prepared.bind
      statement.bind_by_index(0, id)
      statement.bind_by_index(1, tu)
      session.execute(statement)
    end

    # Query and verify timestamps
    result = session.query("SELECT id, created_at FROM cassandra_c_test.uuid_types WHERE id IN ('event1', 'event2', 'event3')")
    rows = result.to_a

    assert_equal 3, rows.length

    # Verify timestamps are extracted correctly from the stored TimeUUIDs
    rows.each do |row|
      id, timeuuid_from_db = row
      case id
      when "event1"
        assert_in_delta time1.to_f, timeuuid_from_db.timestamp.to_f, 0.1
      when "event2"
        assert_in_delta time2.to_f, timeuuid_from_db.timestamp.to_f, 0.1
      when "event3"
        assert_in_delta time3.to_f, timeuuid_from_db.timestamp.to_f, 0.1
      end
    end
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
