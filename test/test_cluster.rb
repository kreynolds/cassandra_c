# frozen_string_literal: true

require "test_helper"

class TestCluster < Minitest::Test
  def test_basic_setters
    cluster = CassandraC::Cluster.new
    cluster.contact_points = "127.0.0.1"
    cluster.port = 9042
    cluster.protocol_version = 4
    cluster.num_threads_io = 2
    cluster.queue_size_io = 8192
    assert true
  end

  def test_local_address
    cluster = CassandraC::Cluster.new
    cluster.local_address = "127.0.0.1"
    assert true
  end

  def test_consistency
    cluster = CassandraC::Cluster.new

    # Test with symbol
    cluster.consistency = :one
    cluster.consistency = :quorum
    cluster.consistency = :all
    cluster.consistency = :local_quorum

    # Direct numeric values should also work for maximum performance
    cluster.consistency = 1  # ONE
    cluster.consistency = 4  # QUORUM

    assert true
  end
  
  def test_round_robin_load_balancing
    cluster = CassandraC::Cluster.new
    cluster.use_round_robin_load_balancing
    assert true
  end
  
  def test_dc_aware_load_balancing
    cluster = CassandraC::Cluster.new
    
    # Test with different datacenter names
    cluster.use_dc_aware_load_balancing("dc1")
    cluster.use_dc_aware_load_balancing("local-dc")
    cluster.use_dc_aware_load_balancing("datacenter1")
    
    assert true
  end
  
  def test_dc_aware_load_balancing_invalid_params
    cluster = CassandraC::Cluster.new
    
    # Test with invalid local_dc type
    assert_raises(TypeError) do
      cluster.use_dc_aware_load_balancing(123)
    end
    
    assert true
  end
end
