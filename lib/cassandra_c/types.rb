require "bigdecimal"
require "securerandom"
require "date"
require "time"

module CassandraC
  module Types
    # All types now use native Ruby equivalents with type hints
    # No wrapper classes needed for basic types except TimeUuid and Time

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

    class TimeUuid
      attr_reader :uuid_string

      def initialize(uuid_string)
        @uuid_string = validate_timeuuid(uuid_string.to_s.downcase)
      end

      def self.generate
        # This will be implemented in C extension
        raise NotImplementedError, "TimeUuid.generate not yet implemented"
      end

      def self.from_time(time)
        # This will be implemented in C extension
        raise NotImplementedError, "TimeUuid.from_time not yet implemented"
      end

      def to_s
        @uuid_string
      end

      def timestamp
        # This will be implemented in C extension
        raise NotImplementedError, "TimeUuid#timestamp not yet implemented"
      end

      def to_time
        timestamp
      end

      def cassandra_typed_timeuuid?
        true
      end

      def ==(other)
        case other
        when TimeUuid
          @uuid_string == other.uuid_string
        when String
          @uuid_string == other.downcase
        else
          false
        end
      end

      def hash
        @uuid_string.hash
      end

      private

      def validate_timeuuid(uuid_str)
        # Basic UUID format validation
        unless uuid_str.match?(/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/)
          raise ArgumentError, "Invalid UUID format: #{uuid_str}"
        end

        # Check if it's a version 1 UUID (TimeUUID)
        version = uuid_str[14].to_i(16)
        unless version == 1
          raise ArgumentError, "UUID must be version 1 (TimeUUID), got version #{version}"
        end

        uuid_str
      end
    end
  end
end
