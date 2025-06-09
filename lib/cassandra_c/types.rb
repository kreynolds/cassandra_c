require "bigdecimal"
require "securerandom"
require "date"
require "time"

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

    # UUID type - Cassandra UUID
    class Uuid
      def initialize(value)
        case value
        when String
          # Validate UUID format
          unless uuid_format?(value)
            raise ArgumentError, "Invalid UUID format: #{value}"
          end
          @value = value.downcase
        else
          raise ArgumentError, "Value must be a string, got #{value.class}"
        end
      end

      def to_s
        @value
      end

      def inspect
        "Uuid(#{@value})"
      end

      def ==(other)
        case other
        when Uuid
          @value == other.to_s
        when String
          @value == other.downcase
        else
          false
        end
      end

      def <=>(other)
        case other
        when Uuid
          @value <=> other.to_s
        when String
          @value <=> other.downcase
        end
      end

      def hash
        @value.hash
      end

      def eql?(other)
        other.is_a?(Uuid) && @value == other.to_s
      end

      # Marker method to identify as a UUID
      def cassandra_typed_uuid?
        true
      end

      # Generate a random UUID v4
      def self.generate
        new(SecureRandom.uuid)
      end

      private

      def uuid_format?(str)
        !!(str =~ /\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i)
      end
    end

    # TimeUUID type - Cassandra TIMEUUID (version 1 UUID)
    class TimeUuid
      def initialize(value = nil)
        if value.nil?
          @value = self.class.generate.to_s
        else
          case value
          when String
            # Validate UUID format and version 1
            unless uuid_format?(value) && timeuuid_version?(value)
              raise ArgumentError, "Invalid TimeUUID format: #{value}"
            end
            @value = value.downcase
          when ::Time
            @value = self.class.from_time(value).to_s
          else
            raise ArgumentError, "Value must be a string, ::Time, or nil, got #{value.class}"
          end
        end
      end

      def to_s
        @value
      end

      def inspect
        "TimeUuid(#{@value})"
      end

      def ==(other)
        case other
        when TimeUuid
          @value == other.to_s
        when String
          @value == other.downcase
        else
          false
        end
      end

      def <=>(other)
        case other
        when TimeUuid
          @value <=> other.to_s
        when String
          @value <=> other.downcase
        end
      end

      def hash
        @value.hash
      end

      def eql?(other)
        other.is_a?(TimeUuid) && @value == other.to_s
      end

      # Extract timestamp from TimeUUID
      def timestamp
        # TimeUUID (version 1) contains timestamp in 100-nanosecond intervals
        # since UUID epoch (October 15, 1582)
        time_low = @value[0, 8].to_i(16)
        time_mid = @value[9, 4].to_i(16)
        time_hi = @value[14, 4].to_i(16) & 0x0FFF # Remove version bits

        # Combine into 60-bit timestamp
        uuid_time = (time_hi << 48) | (time_mid << 32) | time_low

        # Convert from UUID epoch to Unix epoch
        # UUID epoch: Oct 15, 1582 00:00:00 UTC
        # Unix epoch: Jan 1, 1970 00:00:00 UTC
        # Difference: 122192928000000000 (100-nanosecond intervals)
        unix_time_100ns = uuid_time - 0x01B21DD213814000

        # Convert to seconds and microseconds
        unix_seconds = unix_time_100ns / 10_000_000
        microseconds = (unix_time_100ns % 10_000_000) / 10

        ::Time.at(unix_seconds, microseconds)
      end

      # Marker method to identify as a TimeUUID
      def cassandra_typed_timeuuid?
        true
      end

      # Generate a new TimeUUID for current time
      def self.generate(timestamp = ::Time.now)
        from_time(timestamp)
      end

      # Generate TimeUUID from specific timestamp
      def self.from_time(timestamp)
        # Convert Unix timestamp to UUID timestamp (100-nanosecond intervals since UUID epoch)
        unix_time_100ns = (timestamp.to_f * 10_000_000).to_i
        uuid_time = unix_time_100ns + 0x01B21DD213814000

        # Extract time components
        time_low = uuid_time & 0xFFFFFFFF
        time_mid = (uuid_time >> 32) & 0xFFFF
        time_hi = ((uuid_time >> 48) & 0x0FFF) | 0x1000 # Version 1

        # Generate random clock sequence and node
        clock_seq = SecureRandom.random_number(0x4000) | 0x8000 # Variant bits
        node = SecureRandom.random_number(0x1000000000000) | 0x010000000000 # Multicast bit

        # Format as UUID string
        uuid_str = sprintf("%08x-%04x-%04x-%04x-%012x",
          time_low, time_mid, time_hi, clock_seq, node)

        new(uuid_str)
      end

      private

      def uuid_format?(str)
        !!(str =~ /\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i)
      end

      def timeuuid_version?(str)
        # Check if it's version 1 (time-based UUID)
        version_char = str[14]
        version_char == "1"
      end
    end

    # Date type - Cassandra DATE (days since Unix epoch)
    class Date
      EPOCH_DATE = ::Date.new(1970, 1, 1)

      def initialize(value = nil)
        case value
        when nil
          @value = ::Date.today
        when ::Date
          @value = value
        when ::Time
          @value = value.to_date
        when Integer
          # Days since Unix epoch
          @value = EPOCH_DATE + value
        when String
          @value = ::Date.parse(value)
        else
          raise ArgumentError, "Value must be a Date, Time, Integer (days since epoch), String, or nil, got #{value.class}"
        end
      end

      def to_date
        @value
      end

      def to_s
        @value.to_s
      end

      def inspect
        "Date(#{@value})"
      end

      # Convert to days since Unix epoch
      def days_since_epoch
        (@value - EPOCH_DATE).to_i
      end

      def ==(other)
        case other
        when Date
          @value == other.to_date
        when ::Date
          @value == other
        else
          false
        end
      end

      def <=>(other)
        case other
        when Date
          @value <=> other.to_date
        when ::Date
          @value <=> other
        end
      end

      def hash
        @value.hash
      end

      def eql?(other)
        other.is_a?(Date) && @value == other.to_date
      end

      # Comparison operators
      def <(other)
        case other
        when Date
          @value < other.to_date
        when ::Date
          @value < other
        end
      end

      def >(other)
        case other
        when Date
          @value > other.to_date
        when ::Date
          @value > other
        end
      end

      def <=(other)
        case other
        when Date
          @value <= other.to_date
        when ::Date
          @value <= other
        end
      end

      def >=(other)
        case other
        when Date
          @value >= other.to_date
        when ::Date
          @value >= other
        end
      end

      # Marker method to identify as a typed date
      def cassandra_typed_date?
        true
      end

      # Create from days since epoch
      def self.from_days_since_epoch(days)
        new(days)
      end
    end

    # Time type - Cassandra TIME (nanoseconds since midnight)
    class Time
      NANOSECONDS_PER_SECOND = 1_000_000_000
      NANOSECONDS_PER_MINUTE = NANOSECONDS_PER_SECOND * 60
      NANOSECONDS_PER_HOUR = NANOSECONDS_PER_MINUTE * 60

      def initialize(value = nil)
        case value
        when nil
          ruby_time = ::Time.now
          @nanoseconds = time_to_nanoseconds_since_midnight(ruby_time)
        when ::Time
          @nanoseconds = time_to_nanoseconds_since_midnight(value)
        when Integer
          # Nanoseconds since midnight
          @nanoseconds = value % (24 * NANOSECONDS_PER_HOUR) # Wrap around for 24-hour day
        when String
          # Parse time string (e.g., "14:30:45.123456789")
          if value =~ /\A(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?\z/
            hours, minutes, seconds, nanos = $1.to_i, $2.to_i, $3.to_i, ($4 || "0").ljust(9, "0").to_i
            @nanoseconds = hours * NANOSECONDS_PER_HOUR +
              minutes * NANOSECONDS_PER_MINUTE +
              seconds * NANOSECONDS_PER_SECOND +
              nanos
          else
            raise ArgumentError, "Invalid time format: #{value}"
          end
        else
          raise ArgumentError, "Value must be a Time, Integer (nanoseconds), String, or nil, got #{value.class}"
        end
      end

      def nanoseconds_since_midnight
        @nanoseconds
      end

      def to_s
        hours = @nanoseconds / NANOSECONDS_PER_HOUR
        remaining = @nanoseconds % NANOSECONDS_PER_HOUR
        minutes = remaining / NANOSECONDS_PER_MINUTE
        remaining %= NANOSECONDS_PER_MINUTE
        seconds = remaining / NANOSECONDS_PER_SECOND
        nanos = remaining % NANOSECONDS_PER_SECOND

        if nanos == 0
          sprintf("%02d:%02d:%02d", hours, minutes, seconds)
        else
          sprintf("%02d:%02d:%02d.%09d", hours, minutes, seconds, nanos).sub(/0+\z/, "")
        end
      end

      def inspect
        "Time(#{self})"
      end

      def ==(other)
        case other
        when Time
          @nanoseconds == other.nanoseconds_since_midnight
        when ::Time
          @nanoseconds == time_to_nanoseconds_since_midnight(other)
        else
          false
        end
      end

      def <=>(other)
        case other
        when Time
          @nanoseconds <=> other.nanoseconds_since_midnight
        when ::Time
          @nanoseconds <=> time_to_nanoseconds_since_midnight(other)
        end
      end

      def hash
        @nanoseconds.hash
      end

      def eql?(other)
        other.is_a?(Time) && @nanoseconds == other.nanoseconds_since_midnight
      end

      # Comparison operators
      def <(other)
        case other
        when Time
          @nanoseconds < other.nanoseconds_since_midnight
        when ::Time
          @nanoseconds < time_to_nanoseconds_since_midnight(other)
        end
      end

      def >(other)
        case other
        when Time
          @nanoseconds > other.nanoseconds_since_midnight
        when ::Time
          @nanoseconds > time_to_nanoseconds_since_midnight(other)
        end
      end

      def <=(other)
        case other
        when Time
          @nanoseconds <= other.nanoseconds_since_midnight
        when ::Time
          @nanoseconds <= time_to_nanoseconds_since_midnight(other)
        end
      end

      def >=(other)
        case other
        when Time
          @nanoseconds >= other.nanoseconds_since_midnight
        when ::Time
          @nanoseconds >= time_to_nanoseconds_since_midnight(other)
        end
      end

      # Marker method to identify as a typed time
      def cassandra_typed_time?
        true
      end

      # Create from nanoseconds since midnight
      def self.from_nanoseconds_since_midnight(nanoseconds)
        new(nanoseconds)
      end

      private

      def time_to_nanoseconds_since_midnight(time)
        # Get the time components for the current day
        seconds_since_midnight = time.hour * 3600 + time.min * 60 + time.sec
        seconds_since_midnight * NANOSECONDS_PER_SECOND + time.nsec
      end
    end

    # Timestamp type - Cassandra TIMESTAMP (milliseconds since Unix epoch)
    class Timestamp
      def initialize(value = nil)
        case value
        when nil
          @value = ::Time.now
        when ::Time
          @value = value
        when Integer
          # Milliseconds since Unix epoch
          @value = ::Time.at(value / 1000.0)
        when String
          @value = ::Time.parse(value)
        when ::Date
          @value = value.to_time
        else
          raise ArgumentError, "Value must be a Time, Integer (milliseconds), String, Date, or nil, got #{value.class}"
        end
      end

      def to_time
        @value
      end

      def to_s
        @value.to_s
      end

      def inspect
        "Timestamp(#{@value})"
      end

      # Convert to milliseconds since Unix epoch
      def milliseconds_since_epoch
        (@value.to_f * 1000).to_i
      end

      def ==(other)
        case other
        when Timestamp
          @value == other.to_time
        when ::Time
          @value == other
        else
          false
        end
      end

      def <=>(other)
        case other
        when Timestamp
          @value <=> other.to_time
        when ::Time
          @value <=> other
        end
      end

      def hash
        @value.hash
      end

      def eql?(other)
        other.is_a?(Timestamp) && @value == other.to_time
      end

      # Comparison operators
      def <(other)
        case other
        when Timestamp
          @value < other.to_time
        when ::Time
          @value < other
        end
      end

      def >(other)
        case other
        when Timestamp
          @value > other.to_time
        when ::Time
          @value > other
        end
      end

      def <=(other)
        case other
        when Timestamp
          @value <= other.to_time
        when ::Time
          @value <= other
        end
      end

      def >=(other)
        case other
        when Timestamp
          @value >= other.to_time
        when ::Time
          @value >= other
        end
      end

      # Marker method to identify as a typed timestamp
      def cassandra_typed_timestamp?
        true
      end

      # Create from milliseconds since epoch
      def self.from_milliseconds_since_epoch(milliseconds)
        new(milliseconds)
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

# Add conversion methods to String
class String
  def to_cassandra_uuid
    CassandraC::Types::Uuid.new(self)
  end

  def to_cassandra_timeuuid
    CassandraC::Types::TimeUuid.new(self)
  end
end

# Add conversion methods to Time
class Time
  def to_cassandra_timeuuid
    CassandraC::Types::TimeUuid.new(self)
  end

  def to_cassandra_timestamp
    CassandraC::Types::Timestamp.new(self)
  end

  def to_cassandra_time
    CassandraC::Types::Time.new(self)
  end

  def to_cassandra_date
    CassandraC::Types::Date.new(self)
  end
end

# Add conversion methods to Date
class Date
  def to_cassandra_date
    CassandraC::Types::Date.new(self)
  end

  def to_cassandra_timestamp
    CassandraC::Types::Timestamp.new(self)
  end
end

# Add conversion methods to Integer for date/time types
class Integer
  def to_cassandra_date
    CassandraC::Types::Date.new(self)
  end

  def to_cassandra_time
    CassandraC::Types::Time.new(self)
  end

  def to_cassandra_timestamp
    CassandraC::Types::Timestamp.new(self)
  end
end

# Add conversion methods to String for date/time types
class String
  def to_cassandra_date
    CassandraC::Types::Date.new(self)
  end

  def to_cassandra_time
    CassandraC::Types::Time.new(self)
  end

  def to_cassandra_timestamp
    CassandraC::Types::Timestamp.new(self)
  end
end
