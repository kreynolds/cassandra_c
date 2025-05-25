# frozen_string_literal: true

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

    session.close
    @@setup_complete = true
  end
end

# Call setup immediately when test_helper is loaded
TestEnvironment.setup
