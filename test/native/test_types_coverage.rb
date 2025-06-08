# frozen_string_literal: true

require "test_helper"

class TestTypesCoverage < Minitest::Test
  # Test specific edge cases not comprehensively covered elsewhere
  def test_numeric_type_error_handling
    # Test invalid input handling for numeric types
    assert_raises(ArgumentError) { CassandraC::Types::Float.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Types::Double.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Types::VarInt.new("invalid") }
    assert_raises(ArgumentError) { CassandraC::Types::TinyInt.new("invalid") }
  end

  # Test that all expected constants are defined
  def test_type_constants_coverage
    # Verify all integer types have required constants
    [CassandraC::Types::TinyInt, CassandraC::Types::SmallInt,
      CassandraC::Types::Int, CassandraC::Types::BigInt].each do |type_class|
      assert type_class.const_defined?(:BIT_SIZE)
      assert type_class.const_defined?(:MIN_VALUE)
      assert type_class.const_defined?(:MAX_VALUE)
    end
  end
end
