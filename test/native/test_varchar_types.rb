# frozen_string_literal: true

require "test_helper"

class TestVarcharTypes < Minitest::Test
  def test_varchar_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, varchar_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "varchar_test_1", :text)
    statement.bind_by_index(1, "Hello Varchar", :varchar)
    session.execute(statement)

    result = session.query("SELECT varchar_col FROM cassandra_c_test.test_text_types WHERE id = 'varchar_test_1'")
    assert_equal "Hello Varchar", result.to_a.first[0]
  end

  def test_varchar_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, varchar_col) VALUES (:id, :varchar_col)", 2)

    statement.bind_by_name("id", "varchar_test_2", :text)
    statement.bind_by_name("varchar_col", "Varchar by name", :varchar)
    session.execute(statement)

    result = session.query("SELECT varchar_col FROM cassandra_c_test.test_text_types WHERE id = 'varchar_test_2'")
    assert_equal "Varchar by name", result.to_a.first[0]
  end

  def test_varchar_utf8_support
    utf8_text = "Varchar: 日本語 🇯🇵"

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, varchar_col) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "varchar_utf8_test", :text)
    statement.bind_by_index(1, utf8_text, :varchar)
    session.execute(statement)

    result = session.query("SELECT varchar_col FROM cassandra_c_test.test_text_types WHERE id = 'varchar_utf8_test'")
    assert_equal utf8_text, result.to_a.first[0]
  end

  def test_varchar_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, varchar_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "varchar_null_test", :text)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT varchar_col FROM cassandra_c_test.test_text_types WHERE id = 'varchar_null_test'")
    assert_nil result.to_a.first[0]
  end

  def test_varchar_empty_string
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, varchar_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "varchar_empty_test", :text)
    statement.bind_by_index(1, "", :varchar)
    session.execute(statement)

    result = session.query("SELECT varchar_col FROM cassandra_c_test.test_text_types WHERE id = 'varchar_empty_test'")
    assert_equal "", result.to_a.first[0]
  end

  def test_varchar_multibyte_encoding
    multibyte_tests = [
      "2-byte: café résumé naïve",          # 2-byte UTF-8 sequences (é, ï)
      "3-byte: 你好世界 こんにちは",            # 3-byte UTF-8 sequences (Chinese, Japanese)
      "4-byte: 🚀🎉🎊🎈",                   # 4-byte UTF-8 sequences (emoji)
      "Mixed: Ω ≠ ∞ → ← ↑ ↓",              # Mathematical symbols
      "Complex: 🇺🇸🇫🇷🇯🇵 flags"           # Flag emoji (6+ bytes each)
    ]

    multibyte_tests.each_with_index do |text, index|
      assert_equal Encoding::UTF_8, text.encoding
      assert text.valid_encoding?, "Test string should be valid UTF-8"

      statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, varchar_col) VALUES (?, ?)", 2)
      statement.bind_by_index(0, "varchar_multibyte_#{index}", :text)
      statement.bind_by_index(1, text, :varchar)
      session.execute(statement)

      result = session.query("SELECT varchar_col FROM cassandra_c_test.test_text_types WHERE id = 'varchar_multibyte_#{index}'")
      assert_equal text, result.to_a.first[0]
    end
  end
end
