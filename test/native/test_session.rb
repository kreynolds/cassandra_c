# frozen_string_literal: true

require "test_helper"

class TestSession < Minitest::Test
  def test_connects_and_disconnects
    test_session = CassandraC::Native::Session.new
    test_session.connect(cluster)
    assert_kind_of CassandraC::Native::Session, test_session
    assert_nil test_session.close
  end

  def test_connect_returns_a_future
    test_session = CassandraC::Native::Session.new
    future = test_session.connect(cluster, async: true)
    assert_kind_of CassandraC::Native::Future, future
    refute future.ready?
    future.wait
    assert future.ready?
    test_session.close
  end

  def test_should_return_a_prepared_statement_async
    prepared = session.prepare("SELECT * FROM system_schema.tables")
    assert_kind_of CassandraC::Native::Prepared, prepared
  end

  def test_prepared_statement_should_return_client_id
    refute_nil session.client_id
  end

  def test_legacy_session_connect_still_works
    test_session = CassandraC::Native::Session.new
    assert_equal test_session, test_session.connect(cluster)
    assert_nil test_session.close
  end

  def test_prepared_statement_bind_with_parameters
    prepared = session.prepare("SELECT * FROM cassandra_c_test.test_bind_params WHERE keyspace_name = ?")

    # Test bind with parameters array
    statement = prepared.bind(["system"])
    assert_kind_of CassandraC::Native::Statement, statement

    # Test bind without parameters (should still work)
    statement2 = prepared.bind
    assert_kind_of CassandraC::Native::Statement, statement2
  end

  def test_statement_bind_by_index
    prepared = session.prepare("SELECT * FROM cassandra_c_test.test_bind_index WHERE keyspace_name = ?")
    statement = prepared.bind

    # Test binding by index
    assert_equal statement, statement.bind_by_index(0, "system")
  end

  def test_statement_bind_by_name
    prepared = session.prepare("SELECT * FROM cassandra_c_test.test_bind_name WHERE name = :name")
    statement = prepared.bind

    # Test binding by name
    assert_equal statement, statement.bind_by_name("name", "test")
  end

  def test_prepared_statement_bind_with_different_types
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
  end
end
