# frozen_string_literal: true

require "test_helper"

class TestAsciiTypes < Minitest::Test
  def test_ascii_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "ascii_test_1", :text)
    statement.bind_by_index(1, "Hello ASCII", :ascii)
    session.execute(statement)

    result = session.query("SELECT ascii_col FROM cassandra_c_test.test_ascii_types WHERE id = 'ascii_test_1'")
    assert_equal "Hello ASCII", result.to_a.first[0]
  end

  def test_ascii_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (:id, :ascii_col)", 2)

    statement.bind_by_name("id", "ascii_test_2", :text)
    statement.bind_by_name("ascii_col", "ASCII by name", :ascii)
    session.execute(statement)

    result = session.query("SELECT ascii_col FROM cassandra_c_test.test_ascii_types WHERE id = 'ascii_test_2'")
    assert_equal "ASCII by name", result.to_a.first[0]
  end

  def test_ascii_validation_success
    ascii_strings = [
      "Hello World",
      "test123",
      "!@#$%^&*()",
      "ASCII only content",
      "Numbers: 0123456789",
      "Symbols: []{}()_+-=|\\:;\"'<>?/.,~`"
    ]

    ascii_strings.each_with_index do |ascii_text, index|
      statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)", 2)
      statement.bind_by_index(0, "ascii_valid_#{index}", :text)
      statement.bind_by_index(1, ascii_text, :ascii)
      session.execute(statement)

      result = session.query("SELECT ascii_col FROM cassandra_c_test.test_ascii_types WHERE id = 'ascii_valid_#{index}'")
      assert_equal ascii_text, result.to_a.first[0]
    end
  end

  def test_ascii_validation_failure
    non_ascii_strings = [
      "Café",                   # É is > 127
      "résumé",                 # é is > 127
      "naïve",                  # ï is > 127
      "Hello 世界",             # Chinese characters
      "🚀",                     # Emoji
      "Ω",                      # Greek omega
      "½"                       # Fraction symbol
    ]

    non_ascii_strings.each_with_index do |non_ascii_text, index|
      statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)", 2)
      statement.bind_by_index(0, "ascii_fail_#{index}", :text)

      assert_raises(CassandraC::Error) do
        statement.bind_by_index(1, non_ascii_text, :ascii)
      end
    end
  end

  def test_ascii_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "ascii_null_test", :text)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT ascii_col FROM cassandra_c_test.test_ascii_types WHERE id = 'ascii_null_test'")
    assert_nil result.to_a.first[0]
  end

  def test_ascii_empty_string
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "ascii_empty_test", :text)
    statement.bind_by_index(1, "", :ascii)
    session.execute(statement)

    result = session.query("SELECT ascii_col FROM cassandra_c_test.test_ascii_types WHERE id = 'ascii_empty_test'")
    assert_equal "", result.to_a.first[0]
  end

  def test_ascii_character_validation
    # Test characters right at the ASCII boundary
    valid_ascii = (0..127).map(&:chr).join
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "ascii_boundary_test", :text)
    statement.bind_by_index(1, "Valid ASCII: #{valid_ascii.gsub(/[[:cntrl:]]/, "")}", :ascii)
    session.execute(statement)

    # Test invalid character (128 and above)
    invalid_char = 128.chr(Encoding::UTF_8)
    assert_raises(CassandraC::Error) do
      statement.bind_by_index(1, "Invalid: #{invalid_char}", :ascii)
    end
  end
end
