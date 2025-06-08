# frozen_string_literal: true

require "test_helper"

class TestCluster < Minitest::Test
  def test_cluster_configuration
    cluster = CassandraC::Native::Cluster.new

    # Basic configuration - verify methods can be called without exception
    cluster.contact_points = "127.0.0.1"
    cluster.port = 9042
    cluster.protocol_version = 4
    cluster.num_threads_io = 2
    cluster.queue_size_io = 8192
    cluster.local_address = "127.0.0.1"

    # Test passes if no exceptions were raised
    assert true
  end

  def test_consistency_levels
    cluster = CassandraC::Native::Cluster.new

    # Test symbol and numeric consistency levels
    cluster.consistency = :one
    cluster.consistency = :quorum
    cluster.consistency = :all
    cluster.consistency = :local_quorum
    cluster.consistency = 1  # ONE
    cluster.consistency = 4  # QUORUM

    # Test passes if no exceptions were raised
    assert true
  end

  def test_load_balancing_policies
    cluster = CassandraC::Native::Cluster.new

    # Round robin load balancing
    cluster.use_round_robin_load_balancing

    # DC-aware load balancing
    cluster.use_dc_aware_load_balancing("dc1")
  end

  def test_invalid_dc_aware_params
    cluster = CassandraC::Native::Cluster.new

    assert_raises(TypeError) do
      cluster.use_dc_aware_load_balancing(123)
    end
  end
end
