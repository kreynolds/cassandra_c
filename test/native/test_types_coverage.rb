# frozen_string_literal: true

require "test_helper"

class TestTypesCoverage < Minitest::Test
  # Test type handling with native Ruby types
  def test_native_type_usage
    # Test that native Ruby types work as expected
    assert_instance_of Integer, 42
    assert_instance_of Float, 3.14
    assert_instance_of BigDecimal, BigDecimal("123.45")
    assert_instance_of String, "550e8400-e29b-41d4-a716-446655440000"
  end

  # Test TimeUuid specific functionality
  def test_timeuuid_coverage
    # Test TimeUuid initialization
    timeuuid = CassandraC::Native::TimeUuid.new("58e0a7d7-eebc-11d8-9669-0800200c9a66")
    assert_instance_of CassandraC::Native::TimeUuid, timeuuid
    assert timeuuid.cassandra_typed_timeuuid?

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new(123) }
  end
end
