# frozen_string_literal: true

require "test_helper"

class TestRetryPolicy < Minitest::Test
  def test_default_retry_policy
    cluster = CassandraC::Cluster.new
    cluster.use_default_retry_policy
    assert true
  end

  def test_fallthrough_retry_policy
    cluster = CassandraC::Cluster.new
    cluster.use_fallthrough_retry_policy
    assert true
  end

  def test_logging_retry_policy
    cluster = CassandraC::Cluster.new
    cluster.use_logging_retry_policy(:default)
    
    # Test with different policy type
    cluster = CassandraC::Cluster.new
    cluster.use_logging_retry_policy(:fallthrough)
    assert true
  end
  
  def test_invalid_logging_retry_policy
    cluster = CassandraC::Cluster.new
    
    assert_raises(ArgumentError) do
      cluster.use_logging_retry_policy(:invalid_policy)
    end
    
    assert_raises(TypeError) do
      cluster.use_logging_retry_policy(123)
    end
  end
end