# frozen_string_literal: true

# Start SimpleCov only when explicitly requested to avoid interference with C extension compilation
# Coverage is disabled by default and only enabled when COVERAGE=true environment variable is set
# Use: bundle exec rake test_with_coverage OR COVERAGE=true bundle exec rake test
if ENV["COVERAGE"] == "true"
  require "simplecov"
  SimpleCov.start do
    add_filter "/test/"
    add_filter "/vendor/"

    # Coverage thresholds disabled - generate reports but don't enforce minimums
  end
end

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "cassandra_c"

require "minitest/autorun"

# Global test setup - run once for all tests
module TestEnvironment
  @@setup_complete = false

  def self.setup
    return if @@setup_complete

    cluster = CassandraC::Native::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }
    session = CassandraC::Native::Session.new
    session.connect(cluster)

    # Create keyspace
    session.query("CREATE KEYSPACE IF NOT EXISTS cassandra_c_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")

    # Create all test tables
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_bind_params (keyspace_name text PRIMARY KEY, id text)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_bind_index (keyspace_name text PRIMARY KEY, id text)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_bind_name (name text PRIMARY KEY, id text)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_types (id text PRIMARY KEY, text_col text, bool_col boolean, float_col double)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.integer_types (id int PRIMARY KEY, tiny_val tinyint, small_val smallint, int_val int, big_val bigint, var_val varint)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_blob_types (id text PRIMARY KEY, blob_data blob)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.boolean_test (id int PRIMARY KEY, bool_val boolean, nullable_bool boolean)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.boolean_bind_test (id text PRIMARY KEY, value boolean)")
    # Counter tables require special structure - all non-counter columns must be primary key
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.counter_table (id text, category text, page_views counter, unique_visitors counter, PRIMARY KEY (id, category))")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.single_counter (id text PRIMARY KEY, count counter)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.inet_types (id text PRIMARY KEY, ip_address inet, server_ip inet)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.decimal_types (id int PRIMARY KEY, float_val float, double_val double, decimal_val decimal)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.uuid_types (id text PRIMARY KEY, uuid_val uuid, timeuuid_val timeuuid, created_at timeuuid)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.list_types (id text PRIMARY KEY, string_list list<text>, int_list list<int>, mixed_list list<text>)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.set_types (id text PRIMARY KEY, string_set set<text>, int_set set<int>, mixed_set set<text>)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.map_types (id text PRIMARY KEY, string_map map<text, text>, int_map map<text, int>, mixed_map map<text, text>)")
    # Type-hinted collection test tables
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.typed_list_types (id text PRIMARY KEY, tinyint_list list<tinyint>, smallint_list list<smallint>, int_list list<int>, bigint_list list<bigint>, varint_list list<varint>, float_list list<float>, double_list list<double>)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.typed_set_types (id text PRIMARY KEY, tinyint_set set<tinyint>, smallint_set set<smallint>, int_set set<int>, bigint_set set<bigint>, varint_set set<varint>, float_set set<float>, double_set set<double>)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.typed_map_types (id text PRIMARY KEY, text_tinyint_map map<text, tinyint>, text_smallint_map map<text, smallint>, text_int_map map<text, int>, text_bigint_map map<text, bigint>, text_varint_map map<text, varint>, text_float_map map<text, float>, text_double_map map<text, double>)")
    # String type test tables
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_text_types (id text PRIMARY KEY, text_col text, varchar_col varchar)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_ascii_types (id text PRIMARY KEY, ascii_col ascii)")
    session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_mixed_strings (id text PRIMARY KEY, text_col text, ascii_col ascii, varchar_col varchar)")

    session.close
    @@setup_complete = true
  end
end

# Call setup immediately when test_helper is loaded
TestEnvironment.setup

# Shared test helpers with lazy initialization
module TestHelpers
  def cluster
    @_cluster ||= CassandraC::Native::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }
  end

  def session
    @_session ||= begin
      session = CassandraC::Native::Session.new
      session.connect(cluster)
      session
    end
  end
end

# Include helpers in all test classes
class Minitest::Test
  include TestHelpers
end
