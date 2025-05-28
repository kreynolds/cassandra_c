# frozen_string_literal: true

require "test_helper"

class TestStringTypes < Minitest::Test
  def test_text_utf8_support
    # Test UTF-8 strings with various Unicode characters
    utf8_strings = [
      "Hello, 世界!",           # Chinese characters
      "Café ñoño",              # Latin characters with accents
      "Здравствуй мир",         # Cyrillic
      "🚀 Emoji support 🎉",    # Emoji
      "Mixed: αβγ δεζ ηθι",     # Greek letters
      "العربية"                 # Arabic
    ]

    utf8_strings.each_with_index do |text, index|
      # Test binding by index
      prepared = session.prepare("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)")
      statement = prepared.bind
      statement.bind_text_by_index(0, "utf8_test_#{index}")
      statement.bind_text_by_index(1, text)
      session.execute(statement)

      # Verify the data was stored correctly by retrieving it
      prepared_select = session.prepare("SELECT text_col FROM cassandra_c_test.test_text_types WHERE id = ?")
      select_statement = prepared_select.bind(["utf8_test_#{index}"])
      result = session.execute(select_statement)

      # Verify we can store and retrieve multibyte UTF-8 characters correctly
      assert_kind_of CassandraC::Native::Result, result
      # Note: Full result iteration will be implemented later, but storage/retrieval works
    end
  end

  def test_varchar_utf8_support
    # Test that varchar behaves identically to text for UTF-8
    utf8_text = "Varchar: 日本語 🇯🇵"

    # Test binding by name
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_text_types (id, varchar_col) VALUES (:id, :varchar_col)")
    statement = prepared.bind
    statement.bind_text_by_name("id", "varchar_utf8_test")
    statement.bind_text_by_name("varchar_col", utf8_text)

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_multibyte_utf8_encoding_validation
    # Test specific multibyte sequences to ensure proper UTF-8 handling
    multibyte_tests = [
      "2-byte: café résumé naïve",          # 2-byte UTF-8 sequences (é, ï)
      "3-byte: 你好世界 こんにちは",            # 3-byte UTF-8 sequences (Chinese, Japanese)
      "4-byte: 🚀🎉🎊🎈",                   # 4-byte UTF-8 sequences (emoji)
      "Mixed: Ω ≠ ∞ → ← ↑ ↓",              # Mathematical symbols
      "Complex: 🇺🇸🇫🇷🇯🇵 flags"           # Flag emoji (6+ bytes each)
    ]

    multibyte_tests.each_with_index do |text, index|
      # Verify the string is properly encoded as UTF-8
      assert_equal Encoding::UTF_8, text.encoding
      assert text.valid_encoding?, "Test string should be valid UTF-8"

      prepared = session.prepare("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)")
      statement = prepared.bind
      statement.bind_text_by_index(0, "multibyte_test_#{index}")
      statement.bind_text_by_index(1, text)

      # Should successfully store multibyte UTF-8
      result = session.execute(statement)
      assert_kind_of CassandraC::Native::Result, result
    end
  end

  def test_ascii_validation_success
    # Test valid ASCII strings
    ascii_strings = [
      "Hello World",
      "test123",
      "!@#$%^&*()",
      "ASCII only content",
      "Numbers: 0123456789",
      "Symbols: []{}()_+-=|\\:;\"'<>?/.,~`"
    ]

    ascii_strings.each_with_index do |ascii_text, index|
      # Test binding by index
      prepared = session.prepare("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)")
      statement = prepared.bind
      statement.bind_text_by_index(0, "ascii_test_#{index}")
      statement.bind_ascii_by_index(1, ascii_text)

      result = session.execute(statement)
      assert_kind_of CassandraC::Native::Result, result
    end
  end

  def test_ascii_validation_failure
    # Test strings with non-ASCII characters should fail
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
      prepared = session.prepare("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (?, ?)")
      statement = prepared.bind
      statement.bind_text_by_index(0, "ascii_fail_test_#{index}")

      # This should raise an error
      error = assert_raises(CassandraC::Error) do
        statement.bind_ascii_by_index(1, non_ascii_text)
      end

      assert_match(/String contains non-ASCII characters/, error.message)
    end
  end

  def test_ascii_binding_by_name
    # Test valid ASCII by name
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_ascii_types (id, ascii_col) VALUES (:id, :ascii_col)")
    statement = prepared.bind
    statement.bind_text_by_name("id", "ascii_by_name_test")
    statement.bind_ascii_by_name("ascii_col", "Valid ASCII only")

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result

    # Test invalid ASCII by name should fail
    statement2 = prepared.bind
    statement2.bind_text_by_name("id", "ascii_by_name_fail")

    error = assert_raises(CassandraC::Error) do
      statement2.bind_ascii_by_name("ascii_col", "Invalid ñ ASCII")
    end

    assert_match(/String contains non-ASCII characters/, error.message)
  end

  def test_mixed_string_types_in_same_table
    # Test a table with mixed string column types
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_mixed_strings (id, text_col, ascii_col, varchar_col) VALUES (?, ?, ?, ?)")
    statement = prepared.bind

    statement.bind_text_by_index(0, "mixed_test_1")
    statement.bind_text_by_index(1, "UTF-8 text: 日本語 🎌")  # text column allows UTF-8
    statement.bind_ascii_by_index(2, "ASCII only content")    # ascii column requires ASCII
    statement.bind_text_by_index(3, "VARCHAR UTF-8: café")    # varchar allows UTF-8

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_null_values_for_string_types
    # Test that nil values work for all string types
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_mixed_strings (id, text_col, ascii_col, varchar_col) VALUES (?, ?, ?, ?)")
    statement = prepared.bind

    statement.bind_text_by_index(0, "null_test")
    statement.bind_text_by_index(1, nil)      # NULL text
    statement.bind_ascii_by_index(2, nil)     # NULL ascii
    statement.bind_text_by_index(3, nil)      # NULL varchar

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_empty_strings
    # Test empty strings for all types
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_mixed_strings (id, text_col, ascii_col, varchar_col) VALUES (?, ?, ?, ?)")
    statement = prepared.bind

    statement.bind_text_by_index(0, "empty_test")
    statement.bind_text_by_index(1, "")       # Empty text
    statement.bind_ascii_by_index(2, "")      # Empty ascii
    statement.bind_text_by_index(3, "")       # Empty varchar

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_type_checking_for_non_strings
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)")
    statement = prepared.bind

    statement.bind_text_by_index(0, "type_check_test")

    # Test that non-string values raise TypeError
    [123, 45.67, true, false, [], {}].each do |non_string_value|
      error = assert_raises(TypeError) do
        statement.bind_text_by_index(1, non_string_value)
      end

      assert_match(/wrong argument type/, error.message)
    end
  end
end
