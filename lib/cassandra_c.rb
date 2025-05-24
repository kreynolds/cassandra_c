# frozen_string_literal: true

require_relative "cassandra_c/version"
require_relative "cassandra_c/cassandra_c"

module CassandraC
  # Native module contains the low-level C++ driver bindings
  # These classes provide direct access to the underlying Cassandra C++ driver
  # For a more Ruby-idiomatic interface, use the classes in the CassandraC namespace
  module Native
    # All native classes are defined in the C extension
  end
end
