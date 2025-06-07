# frozen_string_literal: true

require "test_helper"
require "bigdecimal"

class TestComprehensiveTypes < Minitest::Test
  # Test all FixedWidthInteger types comprehensively
  def test_tinyint_comprehensive
    # Test initialization and basic methods
    tiny = CassandraC::Types::TinyInt.new(42)
    assert_equal 42, tiny.to_i
    assert_equal "TinyInt(42)", tiny.inspect
    assert tiny.cassandra_typed_integer?

    # Test class constants
    assert_equal 8, CassandraC::Types::TinyInt::BIT_SIZE
    assert_equal(-128, CassandraC::Types::TinyInt::MIN_VALUE)
    assert_equal 127, CassandraC::Types::TinyInt::MAX_VALUE

    # Test normalization (overflow wrapping)
    overflown = CassandraC::Types::TinyInt.new(128)
    assert_equal(-128, overflown.to_i)

    underflown = CassandraC::Types::TinyInt.new(-129)
    assert_equal 127, underflown.to_i

    # Test large overflow
    large = CassandraC::Types::TinyInt.new(300)
    expected = CassandraC::Types::TinyInt.normalize(300)
    assert_equal expected, large.to_i

    # Test arithmetic operations
    result = tiny + 10
    assert_instance_of CassandraC::Types::TinyInt, result
    assert_equal 52, result.to_i

    # Test comparison methods
    other = CassandraC::Types::TinyInt.new(42)
    assert_equal 0, tiny <=> other
    assert tiny == other
    assert tiny == 42
    # eql? is not overridden, so it uses object identity
    refute tiny.eql?(other)  # Different objects
    refute tiny.eql?(43)

    # Test method_missing for non-integer results
    assert_instance_of Integer, tiny.hash
    assert_equal(tiny.to_i < 50, tiny < 50)

    # Test coerce
    coerced = tiny.coerce(100)
    assert_equal [100, 42], coerced

    # Test respond_to_missing
    assert tiny.respond_to?(:+)
    assert tiny.respond_to?(:abs)
    assert tiny.respond_to?(:to_s)
    refute tiny.respond_to?(:non_existent_method)
  end

  def test_smallint_comprehensive
    # Test all the same patterns for SmallInt
    small = CassandraC::Types::SmallInt.new(1000)
    assert_equal 1000, small.to_i
    assert_equal "SmallInt(1000)", small.inspect

    # Test constants
    assert_equal 16, CassandraC::Types::SmallInt::BIT_SIZE
    assert_equal(-32768, CassandraC::Types::SmallInt::MIN_VALUE)
    assert_equal 32767, CassandraC::Types::SmallInt::MAX_VALUE

    # Test overflow
    overflown = CassandraC::Types::SmallInt.new(32768)
    assert_equal(-32768, overflown.to_i)

    # Test arithmetic chaining
    result = small + 100 - 50
    assert_instance_of CassandraC::Types::SmallInt, result
    assert_equal 1050, result.to_i

    # Test comparison with different types
    assert small > 999
    assert small < 1001
    assert small >= 1000
    assert small <= 1000

    # Test large numbers
    large = CassandraC::Types::SmallInt.new(100000)
    expected = CassandraC::Types::SmallInt.normalize(100000)
    assert_equal expected, large.to_i
  end

  def test_int_comprehensive
    int_val = CassandraC::Types::Int.new(1_000_000)
    assert_equal 1_000_000, int_val.to_i
    assert_equal "Int(1000000)", int_val.inspect

    # Test constants
    assert_equal 32, CassandraC::Types::Int::BIT_SIZE
    assert_equal(-2147483648, CassandraC::Types::Int::MIN_VALUE)
    assert_equal 2147483647, CassandraC::Types::Int::MAX_VALUE

    # Test near-boundary values
    max_val = CassandraC::Types::Int.new(2147483647)
    assert_equal 2147483647, max_val.to_i

    overflow = CassandraC::Types::Int.new(2147483648)
    assert_equal(-2147483648, overflow.to_i)

    # Test arithmetic
    doubled = int_val * 2
    assert_instance_of CassandraC::Types::Int, doubled
    assert_equal 2_000_000, doubled.to_i

    # Test modulo operation
    mod_result = int_val % 7
    assert_instance_of CassandraC::Types::Int, mod_result
    assert_equal(1_000_000 % 7, mod_result.to_i)
  end

  def test_bigint_comprehensive
    big = CassandraC::Types::BigInt.new(9_000_000_000_000_000_000)
    assert_equal 9_000_000_000_000_000_000, big.to_i
    assert_equal "BigInt(9000000000000000000)", big.inspect

    # Test constants
    assert_equal 64, CassandraC::Types::BigInt::BIT_SIZE
    assert_equal(-9223372036854775808, CassandraC::Types::BigInt::MIN_VALUE)
    assert_equal 9223372036854775807, CassandraC::Types::BigInt::MAX_VALUE

    # Test very large overflow
    very_large = 10**25
    big_overflow = CassandraC::Types::BigInt.new(very_large)
    expected = CassandraC::Types::BigInt.normalize(very_large)
    assert_equal expected, big_overflow.to_i

    # Test division
    divided = big / 1000
    assert_instance_of CassandraC::Types::BigInt, divided
    assert_equal 9_000_000_000_000_000, divided.to_i

    # Test power operation
    small_big = CassandraC::Types::BigInt.new(10)
    power_result = small_big**3
    assert_instance_of CassandraC::Types::BigInt, power_result
    assert_equal 1000, power_result.to_i
  end

  def test_varint_comprehensive
    # Test with very large number
    huge_number = 12345678901234567890123456789012345678901234567890
    var_int = CassandraC::Types::VarInt.new(huge_number)
    assert_equal huge_number, var_int.to_i
    assert_equal "VarInt(#{huge_number})", var_int.inspect
    assert_equal huge_number.to_s, var_int.to_s

    # Test arithmetic preserves type
    doubled = var_int * 2
    assert_instance_of CassandraC::Types::VarInt, doubled
    assert_equal huge_number * 2, doubled.to_i

    # Test comparison methods
    other_var = CassandraC::Types::VarInt.new(huge_number)
    assert var_int == other_var
    assert var_int == huge_number
    assert_equal 0, var_int <=> other_var

    smaller_var = CassandraC::Types::VarInt.new(huge_number - 1)
    assert var_int > smaller_var
    assert_equal 1, var_int <=> smaller_var

    # Test method delegation
    abs_result = var_int.abs
    assert_instance_of CassandraC::Types::VarInt, abs_result
    assert_equal huge_number, abs_result.to_i

    # Test coerce
    coerced = var_int.coerce(100)
    assert_equal [100, huge_number], coerced

    # Test marker method
    assert var_int.cassandra_typed_integer?

    # Test respond_to_missing
    assert var_int.respond_to?(:+)
    assert var_int.respond_to?(:gcd)
    refute var_int.respond_to?(:invalid_method)

    # Test negative numbers
    negative_var = CassandraC::Types::VarInt.new(-huge_number)
    assert_equal(-huge_number, negative_var.to_i)
    assert negative_var < 0
  end

  def test_float_comprehensive
    float_val = CassandraC::Types::Float.new(3.14159)
    assert_in_delta 3.14159, float_val.to_f, 0.00001
    assert_equal "Float(3.14159)", float_val.inspect
    assert_equal "3.14159", float_val.to_s
    assert float_val.cassandra_typed_float?

    # Test initialization from different types
    from_int = CassandraC::Types::Float.new(42)
    assert_equal 42.0, from_int.to_f

    from_rational = CassandraC::Types::Float.new(Rational(22, 7))
    assert_in_delta(22.0 / 7.0, from_rational.to_f, 0.00001)

    # Test arithmetic operations
    doubled = float_val * 2
    assert_instance_of CassandraC::Types::Float, doubled
    assert_in_delta 6.28318, doubled.to_f, 0.00001

    added = float_val + 1.5
    assert_instance_of CassandraC::Types::Float, added
    assert_in_delta 4.64159, added.to_f, 0.00001

    # Test comparison operations
    other_float = CassandraC::Types::Float.new(3.14159)
    assert float_val == other_float
    assert_in_delta 3.14159, float_val.to_f, 0.00001
    assert_equal 0, float_val <=> other_float

    larger_float = CassandraC::Types::Float.new(3.15)
    assert larger_float > float_val
    assert float_val < larger_float

    # Test comparison methods that don't return Numeric
    # Note: hash values may not be equal due to object differences
    assert_instance_of Integer, float_val.hash
    # eql? uses object identity, not value equality
    refute float_val.eql?(other_float)  # Different objects
    refute float_val.eql?(larger_float)

    # Test coerce
    coerced = float_val.coerce(10.5)
    assert_equal [10.5, 3.14159], coerced

    # Test respond_to_missing
    assert float_val.respond_to?(:round)
    assert float_val.respond_to?(:ceil)
    assert float_val.respond_to?(:floor)
    refute float_val.respond_to?(:fake_method)

    # Test method_missing with non-numeric results
    rounded = float_val.round(2)
    assert_equal 3.14, rounded

    # Test edge cases
    infinity = CassandraC::Types::Float.new(Float::INFINITY)
    assert infinity.to_f.infinite?

    nan = CassandraC::Types::Float.new(Float::NAN)
    assert nan.to_f.nan?

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::Float.new("not_numeric") }
  end

  def test_double_comprehensive
    double_val = CassandraC::Types::Double.new(2.718281828459045)
    assert_in_delta 2.718281828459045, double_val.to_f, 0.000000000000001
    assert_equal "Double(2.718281828459045)", double_val.inspect
    assert_equal "2.718281828459045", double_val.to_s
    assert double_val.cassandra_typed_double?

    # Test high precision arithmetic
    squared = double_val * double_val
    assert_instance_of CassandraC::Types::Double, squared
    expected = 2.718281828459045 * 2.718281828459045
    assert_in_delta expected, squared.to_f, 0.000000000000001

    # Test comparison with self class
    other_double = CassandraC::Types::Double.new(2.718281828459045)
    assert double_val == other_double
    assert_equal 0, double_val <=> other_double

    # Test comparison with different precision
    slightly_different = CassandraC::Types::Double.new(2.718281828459046)
    assert slightly_different > double_val
    assert_equal(-1, double_val <=> slightly_different)

    # Test math operations that exist
    abs_result = double_val.abs
    assert_instance_of CassandraC::Types::Double, abs_result
    assert_in_delta 2.718281828459045, abs_result.to_f, 0.000000000000001

    # Test round operation
    rounded = double_val.round(2)
    assert_instance_of CassandraC::Types::Double, rounded
    assert_in_delta 2.72, rounded.to_f, 0.01

    # Test coerce
    coerced = double_val.coerce(Math::PI)
    assert_equal [Math::PI, 2.718281828459045], coerced

    # Test initialization from BigDecimal
    big_decimal = BigDecimal("2.718281828459045")
    from_big_decimal = CassandraC::Types::Double.new(big_decimal)
    assert_in_delta 2.718281828459045, from_big_decimal.to_f, 0.000000000000001
  end

  def test_decimal_comprehensive
    # Test from string with various precisions
    decimal_str = CassandraC::Types::Decimal.new("123.456789")
    assert_equal BigDecimal("123.456789"), decimal_str.to_d
    assert_equal "123.456789", decimal_str.to_s
    assert_equal 6, decimal_str.scale
    assert_equal 123456789, decimal_str.unscaled_value
    assert decimal_str.cassandra_typed_decimal?

    # Test with explicit scale
    decimal_scale = CassandraC::Types::Decimal.new("123.45", 4)
    assert_equal 4, decimal_scale.scale
    assert_equal BigDecimal("123.45"), decimal_scale.to_d

    # Test from BigDecimal
    big_decimal = BigDecimal("999.123456789012345")
    decimal_big = CassandraC::Types::Decimal.new(big_decimal)
    assert_equal big_decimal, decimal_big.to_d
    assert_equal 15, decimal_big.scale

    # Test from Float
    decimal_float = CassandraC::Types::Decimal.new(3.14159)
    assert_instance_of CassandraC::Types::Decimal, decimal_float
    assert_in_delta 3.14159, decimal_float.to_f, 0.00001

    # Test from Integer
    decimal_int = CassandraC::Types::Decimal.new(42)
    assert_equal BigDecimal("42"), decimal_int.to_d
    assert_equal "42.0", decimal_int.to_s
    assert_equal 42, decimal_int.unscaled_value

    # Test arithmetic operations
    sum = decimal_str + BigDecimal("1.111111")
    assert_instance_of CassandraC::Types::Decimal, sum
    assert_equal BigDecimal("124.567900"), sum.to_d

    product = decimal_str * BigDecimal("2")
    assert_instance_of CassandraC::Types::Decimal, product
    assert_equal BigDecimal("246.913578"), product.to_d

    # Test comparison operations
    other_decimal = CassandraC::Types::Decimal.new("123.456789")
    assert decimal_str == other_decimal
    assert decimal_str == BigDecimal("123.456789")
    assert_in_delta 123.456789, decimal_str.to_f, 0.000001
    assert_equal 0, decimal_str <=> other_decimal

    larger_decimal = CassandraC::Types::Decimal.new("124.0")
    assert larger_decimal > decimal_str
    assert_equal(-1, decimal_str <=> larger_decimal)

    # Test scale handling edge cases
    negative_scale = CassandraC::Types::Decimal.new("123", -5)
    assert_equal 0, negative_scale.scale

    zero_scale = CassandraC::Types::Decimal.new("123", 0)
    assert_equal 0, zero_scale.scale

    # Test very high precision
    high_precision = CassandraC::Types::Decimal.new("0.123456789012345678901234567890")
    # Scale may vary based on BigDecimal precision
    assert high_precision.scale >= 29
    expected_unscaled = BigDecimal("0.123456789012345678901234567890") * (10**high_precision.scale)
    assert_equal expected_unscaled.to_i, high_precision.unscaled_value

    # Test method_missing delegation
    rounded = decimal_str.round(2)
    assert_instance_of CassandraC::Types::Decimal, rounded
    assert_equal BigDecimal("123.46"), rounded.to_d

    truncated = decimal_str.truncate(3)
    assert_instance_of CassandraC::Types::Decimal, truncated
    assert_equal BigDecimal("123.456"), truncated.to_d

    # Test comparison methods that don't return BigDecimal
    assert_instance_of Integer, decimal_str.hash
    # eql? uses object identity for custom classes
    refute decimal_str.eql?(other_decimal)  # Different objects
    refute decimal_str.eql?(larger_decimal)

    # Test coerce
    coerced = decimal_str.coerce(100)
    assert_equal [BigDecimal("100"), BigDecimal("123.456789")], coerced

    # Test respond_to_missing
    assert decimal_str.respond_to?(:+)
    assert decimal_str.respond_to?(:round)
    assert decimal_str.respond_to?(:abs)
    refute decimal_str.respond_to?(:fake_method)

    # Test financial precision example
    money = CassandraC::Types::Decimal.new("1234.56")
    tax_rate = BigDecimal("0.08")
    tax = money * tax_rate
    assert_instance_of CassandraC::Types::Decimal, tax
    assert_equal BigDecimal("98.7648"), tax.to_d

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::Decimal.new(nil) }
    assert_raises(ArgumentError) { CassandraC::Types::Decimal.new([]) }
  end

  def test_uuid_comprehensive
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    uuid = CassandraC::Types::Uuid.new(uuid_str)
    assert_equal uuid_str, uuid.to_s
    assert_equal "Uuid(#{uuid_str})", uuid.inspect
    assert uuid.cassandra_typed_uuid?

    # Test case insensitive initialization
    uppercase_uuid = CassandraC::Types::Uuid.new(uuid_str.upcase)
    assert_equal uuid_str, uppercase_uuid.to_s

    # Test mixed case
    mixed_case = "550E8400-e29b-41D4-a716-446655440000"
    mixed_uuid = CassandraC::Types::Uuid.new(mixed_case)
    assert_equal uuid_str, mixed_uuid.to_s

    # Test equality operations
    other_uuid = CassandraC::Types::Uuid.new(uuid_str)
    assert uuid == other_uuid
    assert uuid == uuid_str
    assert uuid == uuid_str.upcase
    assert uuid.eql?(other_uuid)
    refute uuid.eql?("string")
    refute uuid.eql?(nil)

    # Test hash consistency
    assert_equal uuid.hash, other_uuid.hash
    assert_equal uuid_str.hash, uuid.hash

    # Test comparison operations
    uuid2 = CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716-446655440001")
    assert_equal 0, uuid <=> other_uuid
    assert_equal(-1, uuid <=> uuid2)
    assert_equal 1, uuid2 <=> uuid
    assert_equal(-1, uuid <=> "550e8400-e29b-41d4-a716-446655440001")
    assert_equal 1, uuid <=> "550e8400-e29b-41d4-a716-44665544000"  # Shorter string

    # Test comparison with nil
    assert_nil(uuid <=> nil)

    # Test generation
    generated = CassandraC::Types::Uuid.generate
    assert_instance_of CassandraC::Types::Uuid, generated
    assert generated.cassandra_typed_uuid?
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/, generated.to_s)

    # Generate multiple and ensure they're different
    uuid_set = 10.times.map { CassandraC::Types::Uuid.generate }
    assert_equal 10, uuid_set.uniq.length

    # Test invalid formats
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716") }  # Too short
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new("550e8400-e29b-41d4-a716-446655440000-extra") }  # Too long
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new("gggggggg-e29b-41d4-a716-446655440000") }  # Invalid chars
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new(123) }  # Wrong type
    assert_raises(ArgumentError) { CassandraC::Types::Uuid.new(nil) }  # Nil
  end

  def test_timeuuid_comprehensive
    # Test with known TimeUUID
    timeuuid_str = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)
    assert_equal timeuuid_str, timeuuid.to_s
    assert_equal "TimeUuid(#{timeuuid_str})", timeuuid.inspect
    assert timeuuid.cassandra_typed_timeuuid?

    # Test case insensitive
    uppercase = CassandraC::Types::TimeUuid.new(timeuuid_str.upcase)
    assert_equal timeuuid_str, uppercase.to_s

    # Test timestamp extraction
    timestamp = timeuuid.timestamp
    assert_instance_of Time, timestamp
    assert timestamp.year >= 1970
    assert timestamp.year < 2100

    # Test equality operations
    other_timeuuid = CassandraC::Types::TimeUuid.new(timeuuid_str)
    assert timeuuid == other_timeuuid
    assert timeuuid == timeuuid_str
    assert timeuuid == timeuuid_str.upcase
    assert timeuuid.eql?(other_timeuuid)
    refute timeuuid.eql?("string")

    # Test hash
    assert_equal timeuuid.hash, other_timeuuid.hash
    assert_equal timeuuid_str.hash, timeuuid.hash

    # Test comparison
    timeuuid2 = CassandraC::Types::TimeUuid.new("58e0a7d7-eebc-11d8-9669-0800200c9a67")
    assert_equal 0, timeuuid <=> other_timeuuid
    assert_equal(-1, timeuuid <=> timeuuid2)
    assert_equal 1, timeuuid2 <=> timeuuid
    assert_equal(-1, timeuuid <=> "58e0a7d7-eebc-11d8-9669-0800200c9a67")

    # Test comparison with nil
    assert_nil(timeuuid <=> nil)

    # Test initialization from Time
    time = Time.new(2024, 6, 15, 10, 30, 45)
    from_time = CassandraC::Types::TimeUuid.new(time)
    assert_instance_of CassandraC::Types::TimeUuid, from_time
    extracted_time = from_time.timestamp
    assert_in_delta time.to_f, extracted_time.to_f, 1.0

    # Test auto-generation (nil argument)
    auto_generated = CassandraC::Types::TimeUuid.new
    assert_instance_of CassandraC::Types::TimeUuid, auto_generated
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}\z/, auto_generated.to_s)
    auto_time = auto_generated.timestamp
    current_time = Time.now
    assert_in_delta current_time.to_f, auto_time.to_f, 5.0

    # Test class method generation
    generated = CassandraC::Types::TimeUuid.generate
    assert_instance_of CassandraC::Types::TimeUuid, generated
    assert_match(/\A[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}\z/, generated.to_s)

    # Test from_time class method
    specific_time = Time.new(2024, 1, 1, 12, 0, 0)
    from_time_class = CassandraC::Types::TimeUuid.from_time(specific_time)
    assert_instance_of CassandraC::Types::TimeUuid, from_time_class
    extracted = from_time_class.timestamp
    assert_in_delta specific_time.to_f, extracted.to_f, 1.0

    # Test round-trip precision
    original_time = Time.new(2024, 3, 15, 14, 30, 45.123456)
    round_trip = CassandraC::Types::TimeUuid.from_time(original_time)
    extracted_round_trip = round_trip.timestamp
    assert_in_delta original_time.to_f, extracted_round_trip.to_f, 0.0001

    # Test timestamp components extraction
    time_components = from_time_class.to_s
    assert_equal "1", time_components[14]  # Version 1

    # Test error handling
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new("550e8400-e29b-41d4-a716-446655440000") }  # Wrong version
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new(123) }
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new([]) }

    # Test generation with timestamp argument
    past_time = Time.new(2020, 1, 1)
    past_timeuuid = CassandraC::Types::TimeUuid.generate(past_time)
    past_extracted = past_timeuuid.timestamp
    assert_in_delta past_time.to_f, past_extracted.to_f, 1.0

    # Test uniqueness of generated TimeUUIDs
    timeuuids = 5.times.map { CassandraC::Types::TimeUuid.generate }
    assert_equal 5, timeuuids.uniq.length
  end

  def test_all_conversion_methods
    # Test all Integer conversion methods
    int_val = 42

    tinyint = int_val.to_cassandra_tinyint
    assert_instance_of CassandraC::Types::TinyInt, tinyint
    assert_equal 42, tinyint.to_i

    smallint = int_val.to_cassandra_smallint
    assert_instance_of CassandraC::Types::SmallInt, smallint
    assert_equal 42, smallint.to_i

    int = int_val.to_cassandra_int
    assert_instance_of CassandraC::Types::Int, int
    assert_equal 42, int.to_i

    bigint = int_val.to_cassandra_bigint
    assert_instance_of CassandraC::Types::BigInt, bigint
    assert_equal 42, bigint.to_i

    varint = int_val.to_cassandra_varint
    assert_instance_of CassandraC::Types::VarInt, varint
    assert_equal 42, varint.to_i

    float_from_int = int_val.to_cassandra_float
    assert_instance_of CassandraC::Types::Float, float_from_int
    assert_equal 42.0, float_from_int.to_f

    double_from_int = int_val.to_cassandra_double
    assert_instance_of CassandraC::Types::Double, double_from_int
    assert_equal 42.0, double_from_int.to_f

    decimal_from_int = int_val.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_from_int
    assert_equal BigDecimal("42"), decimal_from_int.to_d

    decimal_with_scale = int_val.to_cassandra_decimal(3)
    assert_equal 3, decimal_with_scale.scale

    # Test Float conversion methods
    float_val = 3.14

    float_from_float = float_val.to_cassandra_float
    assert_instance_of CassandraC::Types::Float, float_from_float
    assert_in_delta 3.14, float_from_float.to_f, 0.001

    double_from_float = float_val.to_cassandra_double
    assert_instance_of CassandraC::Types::Double, double_from_float
    assert_in_delta 3.14, double_from_float.to_f, 0.001

    decimal_from_float = float_val.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_from_float

    decimal_from_float_scale = float_val.to_cassandra_decimal(2)
    assert_equal 2, decimal_from_float_scale.scale

    # Test String conversion methods
    str_val = "123.456"

    decimal_from_string = str_val.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_from_string
    assert_equal BigDecimal("123.456"), decimal_from_string.to_d

    decimal_from_string_scale = str_val.to_cassandra_decimal(5)
    assert_equal 5, decimal_from_string_scale.scale

    # UUID conversions
    uuid_str = "550e8400-e29b-41d4-a716-446655440000"
    uuid_from_string = uuid_str.to_cassandra_uuid
    assert_instance_of CassandraC::Types::Uuid, uuid_from_string
    assert_equal uuid_str, uuid_from_string.to_s

    # TimeUUID conversions
    timeuuid_str = "58e0a7d7-eebc-11d8-9669-0800200c9a66"
    timeuuid_from_string = timeuuid_str.to_cassandra_timeuuid
    assert_instance_of CassandraC::Types::TimeUuid, timeuuid_from_string
    assert_equal timeuuid_str, timeuuid_from_string.to_s

    # Test BigDecimal conversion methods
    big_decimal = BigDecimal("999.123")

    decimal_from_big = big_decimal.to_cassandra_decimal
    assert_instance_of CassandraC::Types::Decimal, decimal_from_big
    assert_equal big_decimal, decimal_from_big.to_d

    decimal_from_big_scale = big_decimal.to_cassandra_decimal(5)
    assert_equal 5, decimal_from_big_scale.scale

    # Test Time conversion methods
    time = Time.new(2024, 1, 1, 12, 0, 0)
    timeuuid_from_time = time.to_cassandra_timeuuid
    assert_instance_of CassandraC::Types::TimeUuid, timeuuid_from_time
    extracted_time = timeuuid_from_time.timestamp
    assert_in_delta time.to_f, extracted_time.to_f, 1.0
  end

  def test_edge_cases_and_error_conditions
    # Test FixedWidthInteger with extreme values
    tiny_max = CassandraC::Types::TinyInt.new(127)
    tiny_min = CassandraC::Types::TinyInt.new(-128)
    assert_equal 127, tiny_max.to_i
    assert_equal(-128, tiny_min.to_i)

    # Test method_missing with various operations
    tiny = CassandraC::Types::TinyInt.new(10)

    # Test operations that return integers
    result = tiny.gcd(15)
    assert_instance_of CassandraC::Types::TinyInt, result
    assert_equal 5, result.to_i

    # Test operations that return non-integers (should not be wrapped)
    string_result = tiny.to_s
    assert_instance_of String, string_result
    # to_s returns object representation, not the numeric value
    assert_match(/TinyInt/, string_result)

    bool_result = tiny.odd?
    assert_equal false, bool_result

    # Test comparison operations (should not be wrapped)
    comparison_result = tiny > 5
    assert_equal true, comparison_result

    # Test VarInt with negative numbers
    negative_huge = -12345678901234567890123456789012345678901234567890
    negative_var = CassandraC::Types::VarInt.new(negative_huge)
    assert_equal negative_huge, negative_var.to_i
    assert negative_var < 0

    # Test Float/Double with special values
    inf_float = CassandraC::Types::Float.new(Float::INFINITY)
    assert inf_float.to_f.infinite?

    neg_inf_double = CassandraC::Types::Double.new(-Float::INFINITY)
    assert neg_inf_double.to_f.infinite?

    nan_float = CassandraC::Types::Float.new(Float::NAN)
    assert nan_float.to_f.nan?

    # Test Decimal with zero scale
    zero_scale_decimal = CassandraC::Types::Decimal.new("123", 0)
    assert_equal 0, zero_scale_decimal.scale
    assert_equal 123, zero_scale_decimal.unscaled_value

    # Test all error conditions comprehensively
    assert_raises(ArgumentError) { CassandraC::Types::TinyInt.new("not_integer") }
    assert_raises(ArgumentError) { CassandraC::Types::SmallInt.new(3.14) }
    assert_raises(ArgumentError) { CassandraC::Types::Int.new(nil) }
    assert_raises(ArgumentError) { CassandraC::Types::BigInt.new([]) }
    assert_raises(ArgumentError) { CassandraC::Types::VarInt.new("string") }
    assert_raises(ArgumentError) { CassandraC::Types::Float.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Types::Double.new("invalid") }

    # Test TimeUUID version validation
    version_2_uuid = "58e0a7d7-eebc-21d8-9669-0800200c9a66"  # Version 2
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new(version_2_uuid) }

    version_4_uuid = "58e0a7d7-eebc-41d8-9669-0800200c9a66"  # Version 4
    assert_raises(ArgumentError) { CassandraC::Types::TimeUuid.new(version_4_uuid) }
  end
end
