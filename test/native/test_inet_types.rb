# frozen_string_literal: true

require "test_helper"
require "ipaddr"

class TestInetTypes < Minitest::Test
  def test_bind_inet_by_index_with_string_ipv4
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "ipv4_string_test")
    statement.bind_inet_by_index(1, "192.168.1.1")
    session.execute(statement)

    result = session.query("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = 'ipv4_string_test'")
    row = result.to_a.first
    assert_equal "192.168.1.1", row[0]
  end

  def test_bind_inet_by_index_with_string_ipv6
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "ipv6_string_test")
    statement.bind_inet_by_index(1, "2001:db8::1")
    session.execute(statement)

    result = session.query("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = 'ipv6_string_test'")
    row = result.to_a.first
    assert_equal "2001:db8::1", row[0]
  end

  def test_bind_inet_by_index_with_ipaddr_ipv4
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "ipv4_ipaddr_test")
    statement.bind_inet_by_index(1, IPAddr.new("10.0.0.1"))
    session.execute(statement)

    result = session.query("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = 'ipv4_ipaddr_test'")
    row = result.to_a.first
    assert_equal "10.0.0.1", row[0]
  end

  def test_bind_inet_by_index_with_ipaddr_ipv6
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "ipv6_ipaddr_test")
    statement.bind_inet_by_index(1, IPAddr.new("fe80::1"))
    session.execute(statement)

    result = session.query("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = 'ipv6_ipaddr_test'")
    row = result.to_a.first
    assert_equal "fe80::1", row[0]
  end

  def test_bind_inet_by_name_with_string_ipv4
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_name("id", "ipv4_name_test")
    statement.bind_inet_by_name("ip_address", "172.16.0.1")
    session.execute(statement)

    result = session.query("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = 'ipv4_name_test'")
    row = result.to_a.first
    assert_equal "172.16.0.1", row[0]
  end

  def test_bind_inet_by_name_with_ipaddr_ipv6
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_name("id", "ipv6_name_test")
    statement.bind_inet_by_name("ip_address", IPAddr.new("2001:db8:85a3::8a2e:370:7334"))
    session.execute(statement)

    result = session.query("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = 'ipv6_name_test'")
    row = result.to_a.first
    assert_equal "2001:db8:85a3::8a2e:370:7334", row[0]
  end

  def test_bind_inet_with_null
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "null_test")
    statement.bind_inet_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = 'null_test'")
    row = result.to_a.first
    assert_nil row[0]
  end

  def test_bind_multiple_inet_columns
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address, server_ip) VALUES (?, ?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "multi_inet_test")
    statement.bind_inet_by_index(1, "192.168.1.100")
    statement.bind_inet_by_index(2, IPAddr.new("10.0.0.200"))
    session.execute(statement)

    result = session.query("SELECT ip_address, server_ip FROM cassandra_c_test.inet_types WHERE id = 'multi_inet_test'")
    row = result.to_a.first
    assert_equal "192.168.1.100", row[0]
    assert_equal "10.0.0.200", row[1]
  end

  def test_invalid_ip_address_string
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "invalid_test")

    error = assert_raises(CassandraC::Error) do
      statement.bind_inet_by_index(1, "not.an.ip.address")
    end
    assert_includes error.message, "Failed to bind inet parameter"
  end

  def test_localhost_addresses
    prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address, server_ip) VALUES (?, ?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "localhost_test")
    statement.bind_inet_by_index(1, "127.0.0.1")
    statement.bind_inet_by_index(2, "::1")
    session.execute(statement)

    result = session.query("SELECT ip_address, server_ip FROM cassandra_c_test.inet_types WHERE id = 'localhost_test'")
    row = result.to_a.first
    assert_equal "127.0.0.1", row[0]
    assert_equal "::1", row[1]
  end

  def test_edge_case_ipv6_addresses
    test_cases = [
      ["compressed_zeros", "::"],
      ["full_ipv6", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"],
      ["mixed_notation", "::ffff:192.0.2.1"]
    ]

    test_cases.each do |test_id, ip_address|
      prepared = session.prepare("INSERT INTO cassandra_c_test.inet_types (id, ip_address) VALUES (?, ?)")
      statement = prepared.bind
      statement.bind_by_index(0, test_id)
      statement.bind_inet_by_index(1, ip_address)
      session.execute(statement)

      prepared_select = session.prepare("SELECT ip_address FROM cassandra_c_test.inet_types WHERE id = ?")
      select_statement = prepared_select.bind([test_id])
      result = session.execute(select_statement)
      row = result.to_a.first
      refute_nil row[0], "IP address should not be nil for #{test_id}"
      # Note: Cassandra may normalize the IP address format
    end
  end
end
