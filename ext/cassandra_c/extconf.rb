# frozen_string_literal: true

require "mkmf"

# Makes all symbols private by default to avoid unintended conflict
# with other gems. To explicitly export symbols you can use RUBY_FUNC_EXPORTED
# selectively, or entirely remove this flag.
# append_cflags("-fvisibility=hidden")

# Check for the Cassandra C/C++ driver
dir_config("cassandra", ["/opt/homebrew"])

unless have_library("cassandra") && have_header("cassandra.h")
  abort "Cassandra C/C++ driver is missing. Please install it."
end

create_makefile("cassandra_c/cassandra_c")
