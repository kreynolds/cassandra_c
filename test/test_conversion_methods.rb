# frozen_string_literal: true

require "test_helper"

class TestConversionMethods < Minitest::Test
  def test_native_type_conversions
    assert_instance_of Integer, 42
    assert_instance_of Float, 3.14
    assert_instance_of BigDecimal, BigDecimal("123.456")

    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    assert_instance_of String, uuid_str
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i, uuid_str)
  end

  def test_timeuuid_functionality
    timeuuid_str = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid = CassandraC::Native::TimeUuid.new(timeuuid_str)

    assert_instance_of CassandraC::Native::TimeUuid, timeuuid
    assert timeuuid.cassandra_typed_timeuuid?
    assert_equal timeuuid_str, timeuuid.to_s
  end

  def test_timeuuid_comparison_and_error_handling
    timeuuid_str = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid1 = CassandraC::Native::TimeUuid.new(timeuuid_str)
    timeuuid2 = CassandraC::Native::TimeUuid.new(timeuuid_str)

    assert_equal timeuuid1, timeuuid2
    assert_equal timeuuid1.hash, timeuuid2.hash
    assert_equal timeuuid1, timeuuid_str

    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new("550e8400-e29b-41d4-a716-446655440000") }
  end
end
