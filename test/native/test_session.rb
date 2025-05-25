# frozen_string_literal: true

require "test_helper"

class TestSession < Minitest::Test
  def self.startup
    cluster = CassandraC::Native::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }
    session = CassandraC::Native::Session.new
    session.connect(cluster)
    session.query("CREATE KEYSPACE IF NOT EXISTS cassandra_c_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.close
  end

  def setup
    @cluster = CassandraC::Native::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }
    self.class.startup unless defined?(@@keyspace_created)
    @@keyspace_created = true

    # Create test tables
    unless defined?(@@tables_created)
      session = CassandraC::Native::Session.new
      session.connect(@cluster)
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_bind_params (keyspace_name text PRIMARY KEY, id text)")
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_bind_index (keyspace_name text PRIMARY KEY, id text)")
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_bind_name (name text PRIMARY KEY, id text)")
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.test_types (id text PRIMARY KEY, text_col text, bool_col boolean, float_col double)")
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.integer_types (id int PRIMARY KEY, tiny_val tinyint, small_val smallint, int_val int, big_val bigint, var_val varint)")
      session.close
      @@tables_created = true
    end
  end

  def test_connects_and_disconnects
    session = CassandraC::Native::Session.new
    session.connect(@cluster)
    assert_kind_of CassandraC::Native::Session, session
    assert_nil session.close
  end

  def test_connect_returns_a_future
    session = CassandraC::Native::Session.new
    future = session.connect(@cluster, async: true)
    assert_kind_of CassandraC::Native::Future, future
    refute future.ready?
    future.wait
    assert future.ready?
    session.close
  end

  def test_should_return_a_prepared_statement_async
    session = CassandraC::Native::Session.new
    session.connect(@cluster)
    prepared = session.prepare("SELECT * FROM system_schema.tables")
    assert_kind_of CassandraC::Native::Prepared, prepared
    session.close
  end

  def test_prepared_statement_should_return_client_id
    session = CassandraC::Native::Session.new
    session.connect(@cluster)
    refute_nil session.client_id
    session.close
  end

  def test_legacy_session_connect_still_works
    session = CassandraC::Native::Session.new
    assert_equal session, session.connect(@cluster)
    assert_nil session.close
  end

  def test_prepared_statement_bind_with_parameters
    session = CassandraC::Native::Session.new
    session.connect(@cluster)
    prepared = session.prepare("SELECT * FROM cassandra_c_test.test_bind_params WHERE keyspace_name = ?")

    # Test bind with parameters array
    statement = prepared.bind(["system"])
    assert_kind_of CassandraC::Native::Statement, statement

    # Test bind without parameters (should still work)
    statement2 = prepared.bind
    assert_kind_of CassandraC::Native::Statement, statement2

    session.close
  end

  def test_statement_bind_by_index
    session = CassandraC::Native::Session.new
    session.connect(@cluster)
    prepared = session.prepare("SELECT * FROM cassandra_c_test.test_bind_index WHERE keyspace_name = ?")
    statement = prepared.bind

    # Test binding by index
    assert_equal statement, statement.bind_by_index(0, "system")

    session.close
  end

  def test_statement_bind_by_name
    session = CassandraC::Native::Session.new
    session.connect(@cluster)
    prepared = session.prepare("SELECT * FROM cassandra_c_test.test_bind_name WHERE name = :name")
    statement = prepared.bind

    # Test binding by name
    assert_equal statement, statement.bind_by_name("name", "test")

    session.close
  end

  def test_prepared_statement_bind_with_different_types
    session = CassandraC::Native::Session.new
    session.connect(@cluster)

    # Test with string
    prepared1 = session.prepare("INSERT INTO cassandra_c_test.test_types (id, text_col) VALUES (?, ?)")
    statement1 = prepared1.bind(["test1", "hello"])
    assert_kind_of CassandraC::Native::Statement, statement1

    # Test with nil
    statement2 = prepared1.bind(["test2", nil])
    assert_kind_of CassandraC::Native::Statement, statement2

    # Test with boolean
    prepared2 = session.prepare("INSERT INTO cassandra_c_test.test_types (id, bool_col) VALUES (?, ?)")
    statement3 = prepared2.bind(["test3", true])
    assert_kind_of CassandraC::Native::Statement, statement3

    # Test with float
    prepared3 = session.prepare("INSERT INTO cassandra_c_test.test_types (id, float_col) VALUES (?, ?)")
    statement4 = prepared3.bind(["test4", 3.14])
    assert_kind_of CassandraC::Native::Statement, statement4

    session.close
  end
end
