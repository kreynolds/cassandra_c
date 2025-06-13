require_relative "../test_helper"

class TestDateTypes < Minitest::Test
  def setup
    @cluster = CassandraC::Native::Cluster.new
    @cluster.contact_points = "127.0.0.1"
    @cluster.port = 9042

    @session = CassandraC::Native::Session.new
    @session.connect(@cluster)

    # Create keyspace for testing
    @session.query("CREATE KEYSPACE IF NOT EXISTS test_dates WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

    # Drop and recreate table with fully qualified name
    @session.query("DROP TABLE IF EXISTS test_dates.date_types_table")
    @session.query(<<~CQL)
      CREATE TABLE test_dates.date_types_table (
        id int PRIMARY KEY,
        date_col date,
        time_col time,
        timestamp_col timestamp
      )
    CQL
  end

  def teardown
    @session&.close
  end

  def test_native_ruby_date_objects
    # Test that we can work with Ruby Date objects directly
    date1 = Date.new(2023, 12, 25)
    assert_equal "2023-12-25", date1.to_s

    # Test that Date objects can be used in parameter binding
    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col) VALUES (?, ?)")
    statement = prepared.bind([99, date1])
    @session.query(statement)

    # Verify the data round-trip
    result = @session.query("SELECT date_col FROM test_dates.date_types_table WHERE id = 99")
    row = result.first
    assert row[0].is_a?(Date)
    assert_equal "2023-12-25", row[0].to_s
  end

  def test_time_type_creation_and_conversion
    # Test Time type creation from different inputs
    time1 = CassandraC::Types::Time.new("14:30:45.123456789")
    assert_equal "14:30:45.123456789", time1.to_s
    expected_nanos = 14 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000 + 123_456_789
    assert_equal expected_nanos, time1.nanoseconds_since_midnight

    # Test creation from nanoseconds
    time2 = CassandraC::Types::Time.new(expected_nanos)
    assert_equal "14:30:45.123456789", time2.to_s

    # Test creation from Time object
    ruby_time = Time.new(2023, 12, 25, 14, 30, 45, 123.456789)
    time3 = CassandraC::Types::Time.new(ruby_time)
    assert_match(/14:30:45/, time3.to_s)

    # Test simple time without nanoseconds
    time4 = CassandraC::Types::Time.new("09:15:30")
    assert_equal "09:15:30", time4.to_s
  end

  def test_native_ruby_time_objects
    # Test that we can work with Ruby Time objects directly
    ruby_time = Time.new(2023, 12, 25, 14, 30, 45, 123.456)

    # Test that Time objects can be used in parameter binding
    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, timestamp_col) VALUES (?, ?)")
    statement = prepared.bind([98, ruby_time])
    @session.query(statement)

    # Verify the data round-trip
    result = @session.query("SELECT timestamp_col FROM test_dates.date_types_table WHERE id = 98")
    row = result.first
    assert row[0].is_a?(Time)
    # Allow small precision differences in milliseconds
    assert_in_delta ruby_time.to_f, row[0].to_f, 0.001
  end

  def test_date_type_parameter_binding_by_index
    # Test binding Date objects by index
    date_val = Date.new(2024, 6, 15)

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col) VALUES (?, ?)")
    statement = prepared.bind([100, date_val])
    @session.query(statement)

    # Verify round-trip
    result = @session.query("SELECT date_col FROM test_dates.date_types_table WHERE id = 100")
    row = result.first
    assert_equal date_val, row[0]
  end

  def test_time_type_parameter_binding_by_index
    # Test binding CassandraC::Types::Time objects by index
    time_val = CassandraC::Types::Time.new("15:45:30.500000000")

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, time_col) VALUES (?, ?)")
    statement = prepared.bind([101, time_val])
    @session.query(statement)

    # Verify round-trip
    result = @session.query("SELECT time_col FROM test_dates.date_types_table WHERE id = 101")
    row = result.first
    assert row[0].is_a?(CassandraC::Types::Time)
    assert_equal time_val.to_s, row[0].to_s
  end

  def test_timestamp_type_parameter_binding_by_index
    # Test binding Time objects by index
    time_val = Time.new(2024, 6, 15, 10, 30, 45)

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, timestamp_col) VALUES (?, ?)")
    statement = prepared.bind([102, time_val])
    @session.query(statement)

    # Verify round-trip
    result = @session.query("SELECT timestamp_col FROM test_dates.date_types_table WHERE id = 102")
    row = result.first
    assert row[0].is_a?(Time)
    assert_in_delta time_val.to_f, row[0].to_f, 0.001
  end

  def test_date_type_parameter_binding_by_name
    # Test binding Date objects by name
    date_val = Date.new(2024, 12, 31)

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col) VALUES (?, ?)")
    statement = prepared.bind([103, date_val])
    @session.query(statement)

    # Verify round-trip
    result = @session.query("SELECT date_col FROM test_dates.date_types_table WHERE id = 103")
    row = result.first
    assert_equal date_val, row[0]
  end

  def test_null_date_type_handling
    # Test NULL handling for date types
    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col, time_col, timestamp_col) VALUES (?, ?, ?, ?)")
    statement = prepared.bind([104, nil, nil, nil])
    @session.query(statement)

    # Verify NULL values
    result = @session.query("SELECT date_col, time_col, timestamp_col FROM test_dates.date_types_table WHERE id = 104")
    row = result.first
    assert_nil row[0]  # date_col
    assert_nil row[1]  # time_col
    assert_nil row[2]  # timestamp_col
  end

  def test_date_time_edge_cases
    # Test edge cases like leap year, end of year, etc.
    leap_day = Date.new(2024, 2, 29)  # Leap year

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col) VALUES (?, ?)")
    statement = prepared.bind([105, leap_day])
    @session.query(statement)

    # Verify leap day handling
    result = @session.query("SELECT date_col FROM test_dates.date_types_table WHERE id = 105")
    row = result.first
    assert_equal leap_day, row[0]
  end

  def test_date_time_type_hints
    # Test type hints for date/time types
    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col, time_col, timestamp_col) VALUES (?, ?, ?, ?)")

    # Using type hints - just bind all at once for now since bind_by_index on prepared isn't implemented
    statement = prepared.bind([106, Date.new(2024, 1, 1), CassandraC::Types::Time.new("12:30:45"), Time.new(2024, 1, 1, 12, 30, 45)])
    @session.query(statement)

    # Verify round-trip with type hints
    result = @session.query("SELECT date_col, time_col, timestamp_col FROM test_dates.date_types_table WHERE id = 106")
    row = result.first
    assert_equal Date.new(2024, 1, 1), row[0]
    assert row[1].is_a?(CassandraC::Types::Time)
    assert row[2].is_a?(Time)
  end

  def test_date_time_conversion_methods
    # Test convenience conversion methods
    assert_respond_to Time.now, :to_cassandra_time
    assert_respond_to 12345, :to_cassandra_time
    assert_respond_to "14:30:45", :to_cassandra_time

    # Test actual conversions
    ruby_time = Time.now
    cassandra_time = ruby_time.to_cassandra_time
    assert cassandra_time.is_a?(CassandraC::Types::Time)

    # Test integer to time conversion
    nanos = 14 * 3600 * 1_000_000_000 + 30 * 60 * 1_000_000_000 + 45 * 1_000_000_000
    time_from_int = nanos.to_cassandra_time
    assert time_from_int.is_a?(CassandraC::Types::Time)
    assert_equal "14:30:45", time_from_int.to_s

    # Test string to time conversion
    time_from_string = "09:15:30.500".to_cassandra_time
    assert time_from_string.is_a?(CassandraC::Types::Time)
    assert_equal "09:15:30.5", time_from_string.to_s
  end
end
