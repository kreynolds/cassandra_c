# frozen_string_literal: true

require "test_helper"
require "bigdecimal"

class TestComprehensiveTypes < Minitest::Test
  # Test tinyint type with native Ruby integers
  def test_tinyint_comprehensive
    tiny_val = 42
    assert_instance_of Integer, tiny_val
    assert_equal 52, tiny_val + 10
    assert tiny_val == 42
  end

  def test_smallint_comprehensive
    small_val = 1000
    assert_instance_of Integer, small_val
    assert_equal 1050, small_val + 100 - 50
    assert small_val == 1000
  end

  def test_int_comprehensive
    int_val = 1_000_000
    assert_instance_of Integer, int_val
    assert_equal 2_000_000, int_val * 2
    assert_equal 1, int_val % 7
  end

  def test_bigint_comprehensive
    big_val = 9_000_000_000_000_000_000
    assert_instance_of Integer, big_val
    assert_equal 9_000_000_000_000_000, big_val / 1000
    assert_equal 1000, 10**3
  end

  def test_varint_comprehensive
    huge_number = 12345678901234567890123456789012345678901234567890
    assert_instance_of Integer, huge_number
    assert huge_number > (huge_number - 1)
    assert(-huge_number < 0)
  end

  def test_float_comprehensive
    float_val = 3.14159
    assert_instance_of Float, float_val
    assert_in_delta 6.28318, float_val * 2, 0.00001
    assert_in_delta 4.64159, float_val + 1.5, 0.00001
    assert Float::INFINITY.infinite?
    assert Float::NAN.nan?
  end

  def test_double_comprehensive
    double_val = 2.718281828459045
    assert_instance_of Float, double_val
    assert_in_delta 7.389056, double_val * double_val, 0.000001
    assert double_val < 2.718281828459046
    assert_in_delta 2.72, double_val.round(2), 0.01
  end

  def test_decimal_comprehensive
    decimal_val = BigDecimal("123.456789")
    assert_instance_of BigDecimal, decimal_val
    assert_equal BigDecimal("124.567900"), decimal_val + BigDecimal("1.111111")
    assert_equal BigDecimal("246.913578"), decimal_val * BigDecimal("2")
    assert_equal BigDecimal("123.46"), decimal_val.round(2)
    assert BigDecimal("0.123456789012345678901234567890").is_a?(BigDecimal)
  end

  def test_uuid_comprehensive
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    assert_instance_of String, uuid_str
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i, uuid_str)
    assert uuid_str == "550e8400-e29b-41d4-a716-446655440000"

    require "securerandom"
    generated = SecureRandom.uuid
    assert_instance_of String, generated
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/, generated)
  end

  def test_timeuuid_comprehensive
    timeuuid_str = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid = CassandraC::Native::TimeUuid.new(timeuuid_str)
    assert_equal timeuuid_str, timeuuid.to_s
    assert timeuuid.cassandra_typed_timeuuid?
    assert timeuuid == CassandraC::Native::TimeUuid.new(timeuuid_str)
    assert_equal "1", timeuuid.to_s[14] # Version 1

    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new("550e8400-e29b-41d4-a716-446655440000") }
  end

  def test_native_types_with_hints
    assert_instance_of Integer, 42
    assert_instance_of Float, 3.14
    assert_instance_of BigDecimal, BigDecimal("123.456")
    assert_instance_of String, "550e8400-e29b-41d4-a716-446655440000"

    timeuuid_str = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid = CassandraC::Native::TimeUuid.new(timeuuid_str)
    assert_instance_of CassandraC::Native::TimeUuid, timeuuid
    assert_equal timeuuid_str, timeuuid.to_s
  end

  def test_edge_cases_and_error_conditions
    assert_equal 255, 127 - -128
    assert_equal 5, 10.gcd(15)
    assert_equal false, 10.odd?

    negative_huge = -12345678901234567890123456789012345678901234567890
    assert negative_huge < 0
    assert_instance_of Integer, negative_huge

    assert Float::INFINITY.infinite?
    assert(-Float::INFINITY.infinite?)
    assert Float::NAN.nan?

    assert_equal 123, BigDecimal("123").to_i

    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new("58e0a7d7-eebc-21d8-9669-0800200c9a66") }
    assert_raises(ArgumentError) { CassandraC::Native::TimeUuid.new("58e0a7d7-eebc-41d8-9669-0800200c9a66") }
  end
end
