# frozen_string_literal: true

# Start SimpleCov only when explicitly requested to avoid interference with C extension compilation
# Use: bundle exec rake test_with_coverage OR COVERAGE=true bundle exec rake test
if ENV["COVERAGE"] == "true"
  require "simplecov"
  SimpleCov.start do
    add_filter "/test/"
    add_filter "/vendor/"

    # Set coverage thresholds to 90% as requested
    minimum_coverage 90
    minimum_coverage_by_file 40  # cassandra_c.rb has some coverage limitations due to C extension loading
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
