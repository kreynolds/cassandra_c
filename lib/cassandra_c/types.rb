require "bigdecimal"

module CassandraC
  module Types
    # Abstract base class for fixed-width signed integers
    class FixedWidthInteger
      def initialize(value)
        unless value.is_a?(Integer)
          raise ArgumentError, "Value must be an integer, got #{value.class}"
        end
        @value = self.class.normalize(value)
      end

      # Normalize value to the bit-size range using modulo for overflow
      def self.normalize(num)
        num %= (1 << self::BIT_SIZE)
        num -= (1 << self::BIT_SIZE) if num >= (1 << (self::BIT_SIZE - 1))
        num
      end

      # Return the underlying Integer value
      def to_i
        @value
      end

      # Delegate most methods to the wrapped integer
      def method_missing(method, ...)
        result = @value.send(method, ...)
        if result.is_a?(Integer) && ![:==, :<=>, :<, :<=, :>, :>=, :eql?, :hash].include?(method)
          self.class.new(result)
        else
          result
        end
      end

      def respond_to_missing?(method, include_private = false)
        @value.respond_to?(method, include_private) || super
      end

      def ==(other)
        @value == (other.is_a?(FixedWidthInteger) ? other.to_i : other)
      end

      def <=>(other)
        @value <=> (other.is_a?(FixedWidthInteger) ? other.to_i : other)
      end

      def inspect
        "#{self.class.name.split("::").last}(#{@value})"
      end

      def coerce(other)
        [other, @value]
      end

      # Marker method to identify as a typed integer
      def cassandra_typed_integer?
        true
      end
    end

    # 8-bit signed integer (-128 to 127) - Cassandra TINYINT
    class TinyInt < FixedWidthInteger
      BIT_SIZE = 8
      MIN_VALUE = -128
      MAX_VALUE = 127
    end

    # 16-bit signed integer (-32,768 to 32,767) - Cassandra SMALLINT
    class SmallInt < FixedWidthInteger
      BIT_SIZE = 16
      MIN_VALUE = -32768
      MAX_VALUE = 32767
    end

    # 32-bit signed integer (-2,147,483,648 to 2,147,483,647) - Cassandra INT
    class Int < FixedWidthInteger
      BIT_SIZE = 32
      MIN_VALUE = -2147483648
      MAX_VALUE = 2147483647
    end

    # 64-bit signed integer - Cassandra BIGINT
    class BigInt < FixedWidthInteger
      BIT_SIZE = 64
      MIN_VALUE = -9223372036854775808
      MAX_VALUE = 9223372036854775807
    end

    # Variable-length integer - Cassandra VARINT (no size limit)
    class VarInt
      def initialize(value)
        unless value.is_a?(Integer)
          raise ArgumentError, "Value must be an integer, got #{value.class}"
        end
        @value = value
      end

      def to_i
        @value
      end

      def to_s
        @value.to_s
      end

      def inspect
        "VarInt(#{@value})"
      end

      def method_missing(method, ...)
        result = @value.send(method, ...)
        if result.is_a?(Integer) && ![:==, :<=>, :<, :<=, :>, :>=, :eql?, :hash].include?(method)
          self.class.new(result)
        else
          result
        end
      end

      def respond_to_missing?(method, include_private = false)
        @value.respond_to?(method, include_private) || super
      end

      def ==(other)
        @value == (other.is_a?(VarInt) ? other.to_i : other)
      end

      def <=>(other)
        @value <=> (other.is_a?(VarInt) ? other.to_i : other)
      end

      def coerce(other)
        [other, @value]
      end

      # Marker method to identify as a typed integer
      def cassandra_typed_integer?
        true
      end
    end

    # 32-bit IEEE 754 floating point - Cassandra FLOAT
    class Float
      def initialize(value)
        unless value.is_a?(Numeric)
          raise ArgumentError, "Value must be numeric, got #{value.class}"
        end
        @value = value.to_f
      end

      def to_f
        @value
      end

      def to_s
        @value.to_s
      end

      def inspect
        "Float(#{@value})"
      end

      def method_missing(method, ...)
        result = @value.send(method, ...)
        if result.is_a?(Numeric) && ![:==, :<=>, :<, :<=, :>, :>=, :eql?, :hash].include?(method)
          self.class.new(result)
        else
          result
        end
      end

      def respond_to_missing?(method, include_private = false)
        @value.respond_to?(method, include_private) || super
      end

      def ==(other)
        @value == (other.is_a?(self.class) ? other.to_f : other)
      end

      def <=>(other)
        @value <=> (other.is_a?(self.class) ? other.to_f : other)
      end

      def coerce(other)
        [other, @value]
      end

      # Marker method to identify as a typed float
      def cassandra_typed_float?
        true
      end
    end

    # 64-bit IEEE 754 floating point - Cassandra DOUBLE
    class Double
      def initialize(value)
        unless value.is_a?(Numeric)
          raise ArgumentError, "Value must be numeric, got #{value.class}"
        end
        @value = value.to_f
      end

      def to_f
        @value
      end

      def to_s
        @value.to_s
      end

      def inspect
        "Double(#{@value})"
      end

      def method_missing(method, ...)
        result = @value.send(method, ...)
        if result.is_a?(Numeric) && ![:==, :<=>, :<, :<=, :>, :>=, :eql?, :hash].include?(method)
          self.class.new(result)
        else
          result
        end
      end

      def respond_to_missing?(method, include_private = false)
        @value.respond_to?(method, include_private) || super
      end

      def ==(other)
        @value == (other.is_a?(self.class) ? other.to_f : other)
      end

      def <=>(other)
        @value <=> (other.is_a?(self.class) ? other.to_f : other)
      end

      def coerce(other)
        [other, @value]
      end

      # Marker method to identify as a typed double
      def cassandra_typed_double?
        true
      end
    end

    # Arbitrary precision decimal - Cassandra DECIMAL
    class Decimal
      def initialize(value, scale = nil)
        case value
        when BigDecimal
          @value = value
          @scale = scale || value.scale
        when String
          @value = BigDecimal(value)
          @scale = scale || @value.scale
        when Numeric
          @value = BigDecimal(value.to_s)
          @scale = scale || @value.scale
        else
          raise ArgumentError, "Value must be numeric or string, got #{value.class}"
        end

        # Ensure scale is non-negative
        @scale = [@scale, 0].max
      end

      def to_d
        @value
      end

      def to_f
        @value.to_f
      end

      def to_s
        @value.to_s("F")
      end

      attr_reader :scale

      def unscaled_value
        (@value * (10**@scale)).to_i
      end

      def inspect
        "Decimal(#{@value}, scale: #{@scale})"
      end

      def method_missing(method, ...)
        result = @value.send(method, ...)
        if result.is_a?(BigDecimal) && ![:==, :<=>, :<, :<=, :>, :>=, :eql?, :hash].include?(method)
          self.class.new(result, @scale)
        else
          result
        end
      end

      def respond_to_missing?(method, include_private = false)
        @value.respond_to?(method, include_private) || super
      end

      def ==(other)
        @value == case other
        when Decimal
          other.to_d
        when BigDecimal
          other
        else
          BigDecimal(other.to_s)
        end
      end

      def <=>(other)
        @value <=> case other
        when Decimal
          other.to_d
        when BigDecimal
          other
        else
          BigDecimal(other.to_s)
        end
      end

      def coerce(other)
        [BigDecimal(other.to_s), @value]
      end

      # Marker method to identify as a typed decimal
      def cassandra_typed_decimal?
        true
      end
    end
  end
end

# Add conversion methods to Integer
class Integer
  def to_cassandra_tinyint
    CassandraC::Types::TinyInt.new(self)
  end

  def to_cassandra_smallint
    CassandraC::Types::SmallInt.new(self)
  end

  def to_cassandra_int
    CassandraC::Types::Int.new(self)
  end

  def to_cassandra_bigint
    CassandraC::Types::BigInt.new(self)
  end

  def to_cassandra_varint
    CassandraC::Types::VarInt.new(self)
  end

  def to_cassandra_float
    CassandraC::Types::Float.new(self)
  end

  def to_cassandra_double
    CassandraC::Types::Double.new(self)
  end

  def to_cassandra_decimal(scale = nil)
    CassandraC::Types::Decimal.new(self, scale)
  end
end

# Add conversion methods to Float
class Float
  def to_cassandra_float
    CassandraC::Types::Float.new(self)
  end

  def to_cassandra_double
    CassandraC::Types::Double.new(self)
  end

  def to_cassandra_decimal(scale = nil)
    CassandraC::Types::Decimal.new(self, scale)
  end
end

# Add conversion methods to String
class String
  def to_cassandra_decimal(scale = nil)
    CassandraC::Types::Decimal.new(self, scale)
  end
end

# Add conversion methods to BigDecimal
class BigDecimal
  def to_cassandra_decimal(scale = nil)
    CassandraC::Types::Decimal.new(self, scale)
  end
end
