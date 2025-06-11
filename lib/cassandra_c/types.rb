require "bigdecimal"
require "securerandom"

module CassandraC
  module Types
    # All types now use native Ruby equivalents with type hints
    # No wrapper classes needed for basic types except TimeUuid

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
