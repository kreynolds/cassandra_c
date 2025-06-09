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
    @session.query("USE test_dates")

    # Drop and recreate table
    @session.query("DROP TABLE IF EXISTS date_types_table")
    @session.query(<<~CQL)
      CREATE TABLE date_types_table (
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

  def test_date_type_creation_and_conversion
    # Test Date type creation from different inputs
    date1 = CassandraC::Types::Date.new(Date.new(2023, 12, 25))
    assert_equal "2023-12-25", date1.to_s
    assert_equal 19716, date1.days_since_epoch  # Days since 1970-01-01

    # Test creation from days since epoch
    date2 = CassandraC::Types::Date.new(19716)
    assert_equal "2023-12-25", date2.to_s

    # Test creation from string
    date3 = CassandraC::Types::Date.new("2023-12-25")
    assert_equal "2023-12-25", date3.to_s

    # Test creation from Time object
    time = Time.new(2023, 12, 25, 14, 30, 0)
    date4 = CassandraC::Types::Date.new(time)
    assert_equal "2023-12-25", date4.to_s

    # Test current date when no parameter
    date5 = CassandraC::Types::Date.new
    assert_equal Date.today.to_s, date5.to_s
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

  def test_timestamp_type_creation_and_conversion
    # Test Timestamp type creation from different inputs
    ruby_time = Time.new(2023, 12, 25, 14, 30, 45, 123.456)
    timestamp1 = CassandraC::Types::Timestamp.new(ruby_time)
    assert_equal ruby_time.to_s, timestamp1.to_s

    # Test creation from milliseconds since epoch
    millis = (ruby_time.to_f * 1000).to_i
    timestamp2 = CassandraC::Types::Timestamp.new(millis)
    assert_equal ruby_time.to_i, timestamp2.to_time.to_i  # Same second

    # Test creation from string
    timestamp3 = CassandraC::Types::Timestamp.new("2023-12-25 14:30:45")
    assert_match(/2023-12-25/, timestamp3.to_s)

    # Test creation from Date
    date = Date.new(2023, 12, 25)
    timestamp4 = CassandraC::Types::Timestamp.new(date)
    assert_match(/2023-12-25/, timestamp4.to_s)

    # Test current time when no parameter
    timestamp5 = CassandraC::Types::Timestamp.new
    assert_equal Time.now.to_i, timestamp5.to_time.to_i, 1  # Within 1 second
  end

  def test_date_type_parameter_binding_by_index
    prepared = @session.prepare("INSERT INTO date_types_table (id, date_col) VALUES (?, ?)")
    statement = prepared.bind([1, CassandraC::Types::Date.new("2023-12-25")])
    @session.query(statement)

    # Test binding by index with specific methods
    statement2 = CassandraC::Native::Statement.new("INSERT INTO date_types_table (id, date_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, 2)
    statement2.bind_date_by_index(1, CassandraC::Types::Date.new("2024-01-01"))
    @session.query(statement2)

    # Verify the data
    result = @session.query("SELECT id, date_col FROM date_types_table WHERE id IN (1, 2)")
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
    prepared = @session.prepare("INSERT INTO date_types_table (id, time_col) VALUES (?, ?)")
    statement = prepared.bind([3, CassandraC::Types::Time.new("14:30:45.123")])
    @session.query(statement)

    # Test binding by index with specific methods
    statement2 = CassandraC::Native::Statement.new("INSERT INTO date_types_table (id, time_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, 4)
    statement2.bind_time_by_index(1, CassandraC::Types::Time.new("09:15:30"))
    @session.query(statement2)

    # Verify the data
    result = @session.query("SELECT id, time_col FROM date_types_table WHERE id IN (3, 4)")
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
    timestamp = CassandraC::Types::Timestamp.new(Time.new(2023, 12, 25, 14, 30, 45))

    prepared = @session.prepare("INSERT INTO date_types_table (id, timestamp_col) VALUES (?, ?)")
    statement = prepared.bind([5, timestamp])
    @session.query(statement)

    # Test binding by index with specific methods
    statement2 = CassandraC::Native::Statement.new("INSERT INTO date_types_table (id, timestamp_col) VALUES (?, ?)", 2)
    statement2.bind_by_index(0, 6)
    statement2.bind_timestamp_by_index(1, CassandraC::Types::Timestamp.new("2024-01-01 12:00:00"))
    @session.query(statement2)

    # Verify the data
    result = @session.query("SELECT id, timestamp_col FROM date_types_table WHERE id IN (5, 6)")
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
    @session.query("DROP TABLE IF EXISTS named_date_table")
    @session.query(<<~CQL)
      CREATE TABLE named_date_table (
        id int PRIMARY KEY,
        birth_date date,
        created_at timestamp,
        daily_time time
      )
    CQL

    statement = CassandraC::Native::Statement.new(
      "INSERT INTO named_date_table (id, birth_date, created_at, daily_time) VALUES (:id, :birth, :created, :time)",
      4
    )

    statement.bind_by_name("id", 1)
    statement.bind_date_by_name("birth", CassandraC::Types::Date.new("1990-05-15"))
    statement.bind_timestamp_by_name("created", CassandraC::Types::Timestamp.new("2023-12-25 14:30:45"))
    statement.bind_time_by_name("time", CassandraC::Types::Time.new("08:30:00"))

    @session.query(statement)

    # Verify the data
    result = @session.query("SELECT * FROM named_date_table WHERE id = 1")
    row = result.first

    assert_equal 1, row[0].to_i
    assert row[1].is_a?(Date)
    assert_equal "1990-05-15", row[1].to_s
    assert row[2].is_a?(Time)
    assert_match(/2023-12-25/, row[2].to_s)
    assert row[3].is_a?(CassandraC::Types::Time)
    assert_equal "08:30:00", row[3].to_s
  end

  def test_date_type_conversion_methods
    # Test conversion methods on different Ruby types
    assert_respond_to 123, :to_cassandra_date
    assert_respond_to Date.today, :to_cassandra_date
    assert_respond_to Time.now, :to_cassandra_date
    assert_respond_to "2023-12-25", :to_cassandra_date

    # Test actual conversions
    date_from_int = 19716.to_cassandra_date
    assert date_from_int.is_a?(CassandraC::Types::Date)
    assert_equal "2023-12-25", date_from_int.to_s

    date_from_ruby_date = Date.new(2023, 12, 25).to_cassandra_date
    assert date_from_ruby_date.is_a?(CassandraC::Types::Date)
    assert_equal "2023-12-25", date_from_ruby_date.to_s

    time_from_string = "14:30:45".to_cassandra_time
    assert time_from_string.is_a?(CassandraC::Types::Time)
    assert_equal "14:30:45", time_from_string.to_s

    timestamp_from_time = Time.new(2023, 12, 25, 14, 30, 45).to_cassandra_timestamp
    assert timestamp_from_time.is_a?(CassandraC::Types::Timestamp)
    assert_match(/2023-12-25/, timestamp_from_time.to_s)
  end

  def test_null_date_type_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO date_types_table (id, date_col, time_col, timestamp_col) VALUES (?, ?, ?, ?)", 4)
    statement.bind_by_index(0, 10)
    statement.bind_date_by_index(1, nil)
    statement.bind_time_by_index(2, nil)
    statement.bind_timestamp_by_index(3, nil)

    @session.query(statement)

    # Verify null values are handled correctly
    result = @session.query("SELECT * FROM date_types_table WHERE id = 10")
    row = result.first

    assert_equal 10, row[0].to_i
    assert_nil row[1]
    assert_nil row[2]
    assert_nil row[3]
  end

  def test_date_type_equality_and_comparison
    date1 = CassandraC::Types::Date.new("2023-12-25")
    date2 = CassandraC::Types::Date.new("2023-12-25")
    date3 = CassandraC::Types::Date.new("2023-12-26")

    assert_equal date1, date2
    assert date1 < date3
    assert date3 > date1

    time1 = CassandraC::Types::Time.new("14:30:45")
    time2 = CassandraC::Types::Time.new("14:30:45")
    time3 = CassandraC::Types::Time.new("15:30:45")

    assert_equal time1, time2
    assert time1 < time3
    assert time3 > time1

    timestamp1 = CassandraC::Types::Timestamp.new("2023-12-25 14:30:45")
    timestamp2 = CassandraC::Types::Timestamp.new("2023-12-25 14:30:45")
    timestamp3 = CassandraC::Types::Timestamp.new("2023-12-25 15:30:45")

    assert_equal timestamp1, timestamp2
    assert timestamp1 < timestamp3
    assert timestamp3 > timestamp1
  end

  def test_date_type_edge_cases
    # Test epoch date (1970-01-01)
    epoch_date = CassandraC::Types::Date.new(0)
    assert_equal "1970-01-01", epoch_date.to_s

    # Test time wraparound (25 hours should wrap to 1 hour)
    time_wrap = CassandraC::Types::Time.new(25 * 3600 * 1_000_000_000)  # 25 hours in nanoseconds
    assert_match(/01:00:00/, time_wrap.to_s)

    # Test very precise time
    precise_time = CassandraC::Types::Time.new("23:59:59.999999999")
    assert_equal "23:59:59.999999999", precise_time.to_s

    # Test timestamp with microsecond precision
    precise_timestamp = CassandraC::Types::Timestamp.new(Time.new(2023, 12, 25, 14, 30, 45, 123.456))
    assert_match(/2023-12-25/, precise_timestamp.to_s)
  end
end
