# frozen_string_literal: true

require "test_helper"

class TestSession < Minitest::Test
  def setup
    @cluster = CassandraC::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }

    @session = CassandraC::Session.new
  end

  def test_connects_and_disconnects
    assert_equal @session, connect!
    assert_nil @session.close
  end

  def test_connect_returns_a_future
    future = connect!(async: true)
    assert_kind_of CassandraC::Future, future
    refute future.ready?
    future.wait
    assert future.ready?
  end

  def test_should_return_a_prepared_statement_async
    connect!
    statement = @session.prepare("SELECT * FROM system_schema.tables")
  end

  def test_prepared_statement_should_return_client_id
    connect!
    refute_nil @session.client_id
  end

  private

  def connect!(async: false)
    @session.connect(@cluster, async: async)
  end
end
