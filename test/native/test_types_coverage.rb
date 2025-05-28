# frozen_string_literal: true

require "test_helper"
require "bigdecimal"

class TestTypesCoverage < Minitest::Test
  # Test Float type thoroughly
  def test_float_type_comprehensive
    float_val = 3.14.to_cassandra_float

    # Test marker method
    assert float_val.cassandra_typed_float?

    # Test to_s
    assert_equal "3.14", float_val.to_s

    # Test inspect
    assert_equal "Float(3.14)", float_val.inspect

    # Test comparison operations
    other_float = 3.14.to_cassandra_float
    assert_equal true, float_val == other_float
    assert_in_delta 3.14, float_val.to_f, 0.001
    assert_equal 0, float_val <=> other_float
    assert_equal 0, float_val <=> 3.14
    assert float_val >= other_float
    assert float_val <= other_float

    # Test arithmetic - result should be Float type
    result = float_val + 1.0
    assert_instance_of CassandraC::Types::Float, result
    assert_in_delta 4.14, result.to_f, 0.001

    # Test coerce
    coerced = float_val.coerce(5.0)
    assert_equal [5.0, 3.14], coerced

    # Test respond_to_missing
    assert float_val.respond_to?(:+)
    assert float_val.respond_to?(:round)

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::Float.new("invalid") }
  end

  # Test Double type thoroughly
  def test_double_type_comprehensive
    double_val = 2.718281828.to_cassandra_double

    # Test marker method
    assert double_val.cassandra_typed_double?

    # Test to_s
    assert_equal "2.718281828", double_val.to_s

    # Test inspect
    assert_equal "Double(2.718281828)", double_val.inspect

    # Test comparison operations
    other_double = 2.718281828.to_cassandra_double
    assert_equal true, double_val == other_double
    assert_in_delta 2.718281828, double_val.to_f, 0.000000001
    assert_equal 0, double_val <=> other_double

    # Test arithmetic
    result = double_val * 2
    assert_instance_of CassandraC::Types::Double, result
    assert_in_delta 5.436563656, result.to_f, 0.000000001

    # Test coerce
    coerced = double_val.coerce(10.0)
    assert_equal [10.0, 2.718281828], coerced

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::Double.new("invalid") }
  end

  # Test Decimal type thoroughly
  def test_decimal_type_comprehensive
    # Test creation from string
    decimal_str = "123.456".to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_str
    assert_equal 123.456, decimal_str.to_f
    assert_equal "123.456", decimal_str.to_s
    assert_equal BigDecimal("123.456"), decimal_str.to_d
    assert_equal 3, decimal_str.scale
    assert_equal 123456, decimal_str.unscaled_value

    # Test creation with explicit scale
    decimal_scale = "12.34".to_cassandra_decimal(4)
    assert_equal 4, decimal_scale.scale

    # Test creation from BigDecimal
    big_decimal = BigDecimal("999.123456789")
    decimal_big = big_decimal.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_big
    assert_equal big_decimal, decimal_big.to_d

    # Test creation from float
    decimal_float = 3.14159.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_float

    # Test creation from integer maintains exact precision
    decimal_int = 42.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_int
    assert_equal BigDecimal("42"), decimal_int.to_d
    assert_equal "42.0", decimal_int.to_s
    assert_equal 42, decimal_int.unscaled_value

    # Test marker method
    assert decimal_str.cassandra_typed_decimal?

    # Test inspect
    assert_equal "Decimal(0.123456e3, scale: 3)", decimal_str.inspect

    # Test comparison operations
    other_decimal = "123.456".to_cassandra_decimal
    assert_equal true, decimal_str == other_decimal
    assert_equal true, decimal_str == BigDecimal("123.456")
    assert_equal BigDecimal("123.456"), decimal_str.to_d
    assert_equal "123.456", decimal_str.to_s
    assert_equal 0, decimal_str <=> other_decimal

    # Test arithmetic maintains exact precision
    result = decimal_str + BigDecimal("1.0")
    assert_instance_of CassandraC::Types::Decimal, result
    assert_equal BigDecimal("124.456"), result.to_d
    assert_equal "124.456", result.to_s

    # Test multiplication maintains precision
    mult_result = decimal_str * BigDecimal("2")
    assert_equal BigDecimal("246.912"), mult_result.to_d
    assert_equal "246.912", mult_result.to_s

    # Test coerce
    coerced = decimal_str.coerce(100)
    assert_equal [BigDecimal("100"), BigDecimal("123.456")], coerced

    # Test respond_to_missing
    assert decimal_str.respond_to?(:+)
    assert decimal_str.respond_to?(:round)

    # Test negative scale handling
    decimal_neg_scale = CassandraC::Types::Decimal.new("123", -2)
    assert_equal 0, decimal_neg_scale.scale

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::Decimal.new(nil) }
  end

  # Test high-precision decimal values that would lose precision as floats
  def test_decimal_high_precision_exact_values
    # Test precise decimal that would lose precision as float (15 decimal places)
    high_precision = "123.123456789012345".to_cassandra_decimal

    # Verify exact string representation is maintained
    assert_equal "123.123456789012345", high_precision.to_s
    assert_equal BigDecimal("123.123456789012345"), high_precision.to_d
    assert_equal 15, high_precision.scale

    # Test that arithmetic maintains full precision
    doubled = high_precision * BigDecimal("2")
    assert_equal BigDecimal("246.24691357802469"), doubled.to_d
    assert_equal "246.24691357802469", doubled.to_s

    # Test unscaled value calculation
    expected_unscaled = BigDecimal("123.123456789012345") * (10**15)
    assert_equal expected_unscaled.to_i, high_precision.unscaled_value

    # Test comparison with exact BigDecimal value
    exact_big_decimal = BigDecimal("123.123456789012345")
    assert_equal true, high_precision == exact_big_decimal
    assert_equal 0, high_precision <=> exact_big_decimal

    # Test round-trip precision (simulating Cassandra storage/retrieval)
    round_trip = high_precision.to_d.to_cassandra_decimal
    assert_equal high_precision.to_s, round_trip.to_s
    assert_equal high_precision.to_d, round_trip.to_d
    assert_equal high_precision.scale, round_trip.scale
    assert_equal high_precision.unscaled_value, round_trip.unscaled_value

    # Test financial precision example (currency with 4 decimal places)
    money_value = "1234567.8901".to_cassandra_decimal
    assert_equal "1234567.8901", money_value.to_s
    assert_equal BigDecimal("1234567.8901"), money_value.to_d
    assert_equal 4, money_value.scale
    assert_equal 12345678901, money_value.unscaled_value

    # Verify no precision loss in financial calculations
    tax_rate = BigDecimal("0.0825")  # 8.25% tax
    total = money_value * tax_rate
    # Verify the calculation is exact based on BigDecimal arithmetic
    expected_tax = BigDecimal("1234567.8901") * BigDecimal("0.0825")
    assert_equal expected_tax, total.to_d
    assert_instance_of CassandraC::Types::Decimal, total

    # Test exact string representation (actual result from calculation)
    assert_equal "101851.85093325", total.to_s
  end

  # Test VarInt type thoroughly
  def test_varint_type_comprehensive
    var_int = 123456789012345678901234567890.to_cassandra_varint

    # Test marker method
    assert var_int.cassandra_typed_integer?

    # Test to_s
    assert_equal "123456789012345678901234567890", var_int.to_s

    # Test inspect
    assert_equal "VarInt(123456789012345678901234567890)", var_int.inspect

    # Test comparison operations
    other_var = 123456789012345678901234567890.to_cassandra_varint
    assert_equal true, var_int == other_var
    assert_equal true, var_int == 123456789012345678901234567890
    assert_equal 0, var_int <=> other_var

    # Test arithmetic
    result = var_int + 1
    assert_instance_of CassandraC::Types::VarInt, result
    assert_equal 123456789012345678901234567891, result.to_i

    # Test coerce
    coerced = var_int.coerce(100)
    assert_equal [100, 123456789012345678901234567890], coerced

    # Test respond_to_missing
    assert var_int.respond_to?(:+)
    assert var_int.respond_to?(:abs)

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::VarInt.new("invalid") }
  end

  # Test FixedWidthInteger edge cases
  def test_fixed_width_integer_edge_cases
    # Test normalize method edge cases
    tiny = CassandraC::Types::TinyInt.new(127)  # Max value
    assert_equal 127, tiny.to_i

    tiny_overflow = CassandraC::Types::TinyInt.new(255)  # Should wrap to -1
    assert_equal(-1, tiny_overflow.to_i)

    # Test coerce method
    coerced = tiny.coerce(50)
    assert_equal [50, 127], coerced

    # Test respond_to_missing
    assert tiny.respond_to?(:+)
    assert tiny.respond_to?(:abs)
    refute tiny.respond_to?(:non_existent_method)

    # Test inspect
    assert_equal "TinyInt(127)", tiny.inspect

    # Test comparison operations that return non-integer results
    assert_equal true, tiny == 127
    assert_equal false, tiny == 128
    assert tiny > 126
    assert tiny < 128

    # Test method_missing with comparison operations (should not wrap in type)
    hash_result = tiny.hash
    assert_instance_of Integer, hash_result

    eql_result = tiny.eql?(127)
    assert [true, false].include?(eql_result)

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::TinyInt.new("invalid") }
  end

  # Test conversion methods for all types
  def test_conversion_methods_coverage
    # Integer conversions
    int_val = 42
    assert_instance_of CassandraC::Types::TinyInt, int_val.to_cassandra_tinyint
    assert_instance_of CassandraC::Types::SmallInt, int_val.to_cassandra_smallint
    assert_instance_of CassandraC::Types::Int, int_val.to_cassandra_int
    assert_instance_of CassandraC::Types::BigInt, int_val.to_cassandra_bigint
    assert_instance_of CassandraC::Types::VarInt, int_val.to_cassandra_varint
    assert_instance_of CassandraC::Types::Float, int_val.to_cassandra_float
    assert_instance_of CassandraC::Types::Double, int_val.to_cassandra_double
    assert_instance_of CassandraC::Types::Decimal, int_val.to_cassandra_decimal

    # Float conversions
    float_val = 3.14
    assert_instance_of CassandraC::Types::Float, float_val.to_cassandra_float
    assert_instance_of CassandraC::Types::Double, float_val.to_cassandra_double
    assert_instance_of CassandraC::Types::Decimal, float_val.to_cassandra_decimal

    # String conversions
    str_val = "123.456"
    assert_instance_of CassandraC::Types::Decimal, str_val.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, str_val.to_cassandra_decimal(2)

    # BigDecimal conversions
    big_val = BigDecimal("999.123")
    assert_instance_of CassandraC::Types::Decimal, big_val.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, big_val.to_cassandra_decimal(5)
  end

  # Test constants are defined
  def test_constants_defined
    assert_equal 8, CassandraC::Types::TinyInt::BIT_SIZE
    assert_equal(-128, CassandraC::Types::TinyInt::MIN_VALUE)
    assert_equal(127, CassandraC::Types::TinyInt::MAX_VALUE)

    assert_equal 16, CassandraC::Types::SmallInt::BIT_SIZE
    assert_equal(-32768, CassandraC::Types::SmallInt::MIN_VALUE)
    assert_equal(32767, CassandraC::Types::SmallInt::MAX_VALUE)

    assert_equal 32, CassandraC::Types::Int::BIT_SIZE
    assert_equal(-2147483648, CassandraC::Types::Int::MIN_VALUE)
    assert_equal(2147483647, CassandraC::Types::Int::MAX_VALUE)

    assert_equal 64, CassandraC::Types::BigInt::BIT_SIZE
    assert_equal(-9223372036854775808, CassandraC::Types::BigInt::MIN_VALUE)
    assert_equal(9223372036854775807, CassandraC::Types::BigInt::MAX_VALUE)
  end
end
