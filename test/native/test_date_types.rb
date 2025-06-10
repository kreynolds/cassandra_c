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
    # Check that we get back a time within the same second (millisecond precision loss is expected)
    assert_equal ruby_time.to_i, row[0].to_i
  end

  def test_date_type_parameter_binding_by_index
    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col) VALUES (?, ?)")
    statement = prepared.bind([1, Date.new(2023, 12, 25)])
    @session.query(statement)

    # Test binding by index with specific methods
    statement2 = CassandraC::Native::Statement.new("INSERT INTO test_dates.date_types_table (id, date_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, 2)
    statement2.bind_date_by_index(1, Date.new(2024, 1, 1))
    @session.query(statement2)

    # Verify the data
    result = @session.query("SELECT id, date_col FROM test_dates.date_types_table WHERE id IN (1, 2)")
    rows = result.to_a
    assert_equal 2, rows.length

    # Find row with id=1
    row1 = rows.find { |r| r[0].to_i == 1 }
    assert row1[1].is_a?(Date)
    assert_equal "2023-12-25", row1[1].to_s

    # Find row with id=2
    row2 = rows.find { |r| r[0].to_i == 2 }
    assert row2[1].is_a?(Date)
    assert_equal "2024-01-01", row2[1].to_s
  end

  def test_time_type_parameter_binding_by_index
    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, time_col) VALUES (?, ?)")
    statement = prepared.bind([3, CassandraC::Types::Time.new("14:30:45.123")])
    @session.query(statement)

    # Test binding by index with specific methods
    statement2 = CassandraC::Native::Statement.new("INSERT INTO test_dates.date_types_table (id, time_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, 4)
    statement2.bind_time_by_index(1, CassandraC::Types::Time.new("09:15:30"))
    @session.query(statement2)

    # Verify the data
    result = @session.query("SELECT id, time_col FROM test_dates.date_types_table WHERE id IN (3, 4)")
    rows = result.to_a
    assert_equal 2, rows.length

    # Find row with id=3
    row1 = rows.find { |r| r[0].to_i == 3 }
    assert row1[1].is_a?(CassandraC::Types::Time)
    assert_match(/14:30:45/, row1[1].to_s)

    # Find row with id=4
    row2 = rows.find { |r| r[0].to_i == 4 }
    assert row2[1].is_a?(CassandraC::Types::Time)
    assert_equal "09:15:30", row2[1].to_s
  end

  def test_timestamp_type_parameter_binding_by_index
    timestamp = Time.new(2023, 12, 25, 14, 30, 45)

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, timestamp_col) VALUES (?, ?)")
    statement = prepared.bind([5, timestamp])
    @session.query(statement)

    # Test binding by index with specific methods
    statement2 = CassandraC::Native::Statement.new("INSERT INTO test_dates.date_types_table (id, timestamp_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, 6)
    statement2.bind_timestamp_by_index(1, Time.parse("2024-01-01 12:00:00"))
    @session.query(statement2)

    # Verify the data
    result = @session.query("SELECT id, timestamp_col FROM test_dates.date_types_table WHERE id IN (5, 6)")
    rows = result.to_a
    assert_equal 2, rows.length

    # Find row with id=5
    row1 = rows.find { |r| r[0].to_i == 5 }
    assert row1[1].is_a?(Time)
    assert_match(/2023-12-25/, row1[1].to_s)

    # Find row with id=6
    row2 = rows.find { |r| r[0].to_i == 6 }
    assert row2[1].is_a?(Time)
    assert_match(/2024-01-01/, row2[1].to_s)
  end

  def test_date_type_parameter_binding_by_name
    @session.query("DROP TABLE IF EXISTS test_dates.named_date_table")
    @session.query(<<~CQL)
      CREATE TABLE test_dates.named_date_table (
        id int PRIMARY KEY,
        birth_date date,
        created_at timestamp,
        daily_time time
      )
    CQL

    statement = CassandraC::Native::Statement.new(
      "INSERT INTO test_dates.named_date_table (id, birth_date, created_at, daily_time) VALUES (:id, :birth, :created, :time)",
      4
    )

    statement.bind_by_name("id", 1)
    statement.bind_date_by_name("birth", Date.new(1990, 5, 15))
    statement.bind_timestamp_by_name("created", Time.parse("2023-12-25 14:30:45"))
    statement.bind_time_by_name("time", CassandraC::Types::Time.new("08:30:00"))

    @session.query(statement)

    # Verify the data
    result = @session.query("SELECT * FROM test_dates.named_date_table WHERE id = 1")
    row = result.first

    assert_equal 1, row[0].to_i
    assert row[1].is_a?(Date)
    assert_equal "1990-05-15", row[1].to_s
    assert row[2].is_a?(Time)
    assert_match(/2023-12-25/, row[2].to_s)
    assert row[3].is_a?(CassandraC::Types::Time)
    assert_equal "08:30:00", row[3].to_s
  end

  def test_time_type_conversion_methods
    # Test conversion methods for TIME type only
    assert_respond_to "14:30:45", :to_cassandra_time
    assert_respond_to Time.now, :to_cassandra_time
    assert_respond_to 123456789, :to_cassandra_time

    # Test actual conversions
    time_from_string = "14:30:45".to_cassandra_time
    assert time_from_string.is_a?(CassandraC::Types::Time)
    assert_equal "14:30:45", time_from_string.to_s

    time_from_ruby_time = Time.new(2023, 12, 25, 14, 30, 45).to_cassandra_time
    assert time_from_ruby_time.is_a?(CassandraC::Types::Time)
    assert_match(/14:30:45/, time_from_ruby_time.to_s)
  end

  def test_null_date_type_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO test_dates.date_types_table (id, date_col, time_col, timestamp_col) VALUES (?, ?, ?, ?)", 4)
    statement.bind_by_index(0, 10)
    statement.bind_date_by_index(1, nil)
    statement.bind_time_by_index(2, nil)
    statement.bind_timestamp_by_index(3, nil)

    @session.query(statement)

    # Verify null values are handled correctly
    result = @session.query("SELECT * FROM test_dates.date_types_table WHERE id = 10")
    row = result.first

    assert_equal 10, row[0].to_i
    assert_nil row[1]
    assert_nil row[2]
    assert_nil row[3]
  end

  def test_date_time_equality_and_comparison
    # Test Ruby Date objects
    date1 = Date.new(2023, 12, 25)
    date2 = Date.new(2023, 12, 25)
    date3 = Date.new(2023, 12, 26)

    assert_equal date1, date2
    assert date1 < date3
    assert date3 > date1

    # Test CassandraC::Types::Time objects (only type that remains wrapped)
    time1 = CassandraC::Types::Time.new("14:30:45")
    time2 = CassandraC::Types::Time.new("14:30:45")
    time3 = CassandraC::Types::Time.new("15:30:45")

    assert_equal time1, time2
    assert time1 < time3
    assert time3 > time1

    # Test Ruby Time objects
    timestamp1 = Time.new(2023, 12, 25, 14, 30, 45)
    timestamp2 = Time.new(2023, 12, 25, 14, 30, 45)
    timestamp3 = Time.new(2023, 12, 25, 15, 30, 45)

    assert_equal timestamp1, timestamp2
    assert timestamp1 < timestamp3
    assert timestamp3 > timestamp1
  end

  def test_date_time_edge_cases
    # Test epoch date (1970-01-01) with Ruby Date
    epoch_date = Date.new(1970, 1, 1)

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, date_col) VALUES (?, ?)")
    statement = prepared.bind([97, epoch_date])
    @session.query(statement)

    result = @session.query("SELECT date_col FROM test_dates.date_types_table WHERE id = 97")
    row = result.first
    assert row[0].is_a?(Date)
    assert_equal "1970-01-01", row[0].to_s

    # Test time wraparound (25 hours should wrap to 1 hour)
    time_wrap = CassandraC::Types::Time.new(25 * 3600 * 1_000_000_000)  # 25 hours in nanoseconds
    assert_match(/01:00:00/, time_wrap.to_s)

    # Test very precise time
    precise_time = CassandraC::Types::Time.new("23:59:59.999999999")
    assert_equal "23:59:59.999999999", precise_time.to_s

    # Test timestamp with microsecond precision using Ruby Time
    precise_timestamp = Time.new(2023, 12, 25, 14, 30, 45, 123.456)

    prepared = @session.prepare("INSERT INTO test_dates.date_types_table (id, timestamp_col) VALUES (?, ?)")
    statement = prepared.bind([96, precise_timestamp])
    @session.query(statement)

    result = @session.query("SELECT timestamp_col FROM test_dates.date_types_table WHERE id = 96")
    row = result.first
    assert row[0].is_a?(Time)
    assert_equal precise_timestamp.to_i, row[0].to_i  # Same second
  end
end
