# frozen_string_literal: true

require "test_helper"

class TestConversionMethods < Minitest::Test
  # Test String to UUID conversion
  def test_string_to_cassandra_uuid
    uuid_string = "550e8400-e29b-41d4-a716-446655440000"
    uuid = uuid_string.to_cassandra_uuid

    assert_instance_of CassandraC::Types::Uuid, uuid
    assert_equal uuid_string, uuid.to_s
    assert uuid.cassandra_typed_uuid?
  end

  # Test String to TimeUuid conversion
  def test_string_to_cassandra_timeuuid
    # Use a valid TimeUUID (version 1)
    timeuuid_string = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid = timeuuid_string.to_cassandra_timeuuid

    assert_instance_of CassandraC::Types::TimeUuid, timeuuid
    assert_equal timeuuid_string, timeuuid.to_s
    assert timeuuid.cassandra_typed_timeuuid?
  end

  # Test Time to TimeUuid conversion
  def test_time_to_cassandra_timeuuid
    time = Time.new(2024, 1, 1, 12, 0, 0)
    timeuuid = time.to_cassandra_timeuuid

    assert_instance_of CassandraC::Types::TimeUuid, timeuuid
    assert timeuuid.cassandra_typed_timeuuid?

    # Verify the timestamp can be extracted and is close to the original time
    extracted_time = timeuuid.timestamp
    assert_in_delta time.to_f, extracted_time.to_f, 1.0  # Within 1 second
  end

  # Test UUID generation
  def test_uuid_generation
    uuid = CassandraC::Types::Uuid.generate

    assert_instance_of CassandraC::Types::Uuid, uuid
    assert uuid.cassandra_typed_uuid?
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/, uuid.to_s)
  end

  # Test TimeUUID generation
  def test_timeuuid_generation
    timeuuid = CassandraC::Types::TimeUuid.generate

    assert_instance_of CassandraC::Types::TimeUuid, timeuuid
    assert timeuuid.cassandra_typed_timeuuid?
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}\z/, timeuuid.to_s)
  end

  # Test TimeUUID from_time class method
  def test_timeuuid_from_time
    time = Time.new(2024, 6, 15, 10, 30, 45)
    timeuuid = CassandraC::Types::TimeUuid.from_time(time)

    assert_instance_of CassandraC::Types::TimeUuid, timeuuid
    assert timeuuid.cassandra_typed_timeuuid?

    # Verify the timestamp extraction
    extracted_time = timeuuid.timestamp
    assert_in_delta time.to_f, extracted_time.to_f, 1.0
  end

  # Test TimeUUID with no arguments (auto-generation)
  def test_timeuuid_auto_generation
    timeuuid = CassandraC::Types::TimeUuid.new

    assert_instance_of CassandraC::Types::TimeUuid, timeuuid
    assert timeuuid.cassandra_typed_timeuuid?
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}\z/, timeuuid.to_s)

    # Should be close to current time
    current_time = Time.now
    extracted_time = timeuuid.timestamp
    assert_in_delta current_time.to_f, extracted_time.to_f, 5.0  # Within 5 seconds
  end

  # Test UUID comparison and hash methods
  def test_uuid_comparison_and_hash
    uuid1 = CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716-446655440000")
    uuid2 = CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716-446655440000")
    uuid3 = CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716-446655440001")

    # Test equality
    assert_equal uuid1, uuid2
    refute_equal uuid1, uuid3

    # Test eql?
    assert uuid1.eql?(uuid2)
    refute uuid1.eql?(uuid3)
    refute uuid1.eql?("string")

    # Test hash
    assert_equal uuid1.hash, uuid2.hash
    refute_equal uuid1.hash, uuid3.hash

    # Test comparison with string
    assert_equal uuid1, "550e8400-e29b-41d4-a716-446655440000"
    assert_equal uuid1, "550E8400-E29B-41D4-A716-446655440000"  # Case insensitive

    # Test <=> operator
    assert_equal 0, uuid1 <=> uuid2
    assert_equal(-1, uuid1 <=> uuid3)
    assert_equal 1, uuid3 <=> uuid1
    assert_equal(-1, uuid1 <=> "550e8400-e29b-41d4-a716-446655440001")
  end

  # Test TimeUUID comparison and hash methods
  def test_timeuuid_comparison_and_hash
    timeuuid1 = CassandraC::Types::TimeUuid.new("58e0a7d7-eebc-11d8-9669-0800200c9a66")
    timeuuid2 = CassandraC::Types::TimeUuid.new("58e0a7d7-eebc-11d8-9669-0800200c9a66")
    timeuuid3 = CassandraC::Types::TimeUuid.new("58e0a7d7-eebc-11d8-9669-0800200c9a67")

    # Test equality
    assert_equal timeuuid1, timeuuid2
    refute_equal timeuuid1, timeuuid3

    # Test eql?
    assert timeuuid1.eql?(timeuuid2)
    refute timeuuid1.eql?(timeuuid3)
    refute timeuuid1.eql?("string")

    # Test hash
    assert_equal timeuuid1.hash, timeuuid2.hash
    refute_equal timeuuid1.hash, timeuuid3.hash

    # Test comparison with string
    assert_equal timeuuid1, "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    assert_equal timeuuid1, "58E0A7D7-EEBC-11D8-9669-0800200C9A66"  # Case insensitive

    # Test <=> operator
    assert_equal 0, timeuuid1 <=> timeuuid2
    assert_equal(-1, timeuuid1 <=> timeuuid3)
    assert_equal 1, timeuuid3 <=> timeuuid1
    assert_equal(-1, timeuuid1 <=> "58e0a7d7-eebc-11d8-9669-0800200c9a67")
  end

  # Test error handling for invalid UUIDs
  def test_uuid_error_handling
    # Invalid UUID format
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new("invalid-uuid") }
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716") }  # Too short
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716-446655440000-extra") }  # Too long

    # Non-string input
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new(123) }
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new(nil) }
  end

  # Test error handling for invalid TimeUUIDs
  def test_timeuuid_error_handling
    # Invalid UUID format
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new("invalid-timeuuid") }

    # Valid UUID format but not version 1
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new("550e8400-e29b-41d4-a716-446655440000") }  # Version 4

    # Non-string, non-Time, non-nil input
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new(123) }
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new([]) }
  end

  # Test TimeUUID timestamp extraction edge cases
  def test_timeuuid_timestamp_extraction
    # Test with a known TimeUUID and expected timestamp
    # This TimeUUID was generated for a specific time
    known_timeuuid = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid = CassandraC::Types::TimeUuid.new(known_timeuuid)

    # Extract timestamp and verify it's reasonable (after year 1970, before year 2100)
    timestamp = timeuuid.timestamp
    assert timestamp.year >= 1970
    assert timestamp.year < 2100

    # Test round-trip: create TimeUUID from time, then extract timestamp
    original_time = Time.new(2024, 3, 15, 14, 30, 45.123456)  # Include fractional seconds
    timeuuid_from_time = CassandraC::Types::TimeUuid.from_time(original_time)
    extracted_time = timeuuid_from_time.timestamp

    # Should be very close (within 100 microseconds due to TimeUUID precision)
    assert_in_delta original_time.to_f, extracted_time.to_f, 0.0001
  end

  # Test duplicate string conversion methods for Decimal
  def test_duplicate_string_conversion_methods
    # Both methods should exist and work the same way
    decimal1 = "123.45".to_cassandra_decimal
    decimal2 = "123.45".to_cassandra_decimal(4)

    assert_instance_of CassandraC::Types::Decimal, decimal1
    assert_instance_of CassandraC::Types::Decimal, decimal2
    assert_equal 2, decimal1.scale
    assert_equal 4, decimal2.scale
  end
end
