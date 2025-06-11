# frozen_string_literal: true

require "test_helper"

class TestTextTypes < Minitest::Test
  def test_text_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "text_test_1", :text)
    statement.bind_by_index(1, "Hello World", :text)
    session.execute(statement)

    result = session.query("SELECT text_col FROM cassandra_c_test.test_text_types WHERE id = 'text_test_1'")
    assert_equal "Hello World", result.to_a.first[0]
  end

  def test_text_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (:id, :text_col)", 2)

    statement.bind_by_name("id", "text_test_2", :text)
    statement.bind_by_name("text_col", "Test by name", :text)
    session.execute(statement)

    result = session.query("SELECT text_col FROM cassandra_c_test.test_text_types WHERE id = 'text_test_2'")
    assert_equal "Test by name", result.to_a.first[0]
  end

  def test_text_utf8_support
    utf8_strings = [
      "Hello, 世界!",           # Chinese characters
      "Café ñoño",              # Latin characters with accents
      "Здравствуй мир",         # Cyrillic
      "🚀 Emoji support 🎉",    # Emoji
      "Mixed: αβγ δεζ ηθι",     # Greek letters
      "العربية"                 # Arabic
    ]

    utf8_strings.each_with_index do |text, index|
      statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)", 2)
      statement.bind_by_index(0, "utf8_test_#{index}", :text)
      statement.bind_by_index(1, text, :text)
      session.execute(statement)

      result = session.query("SELECT text_col FROM cassandra_c_test.test_text_types WHERE id = 'utf8_test_#{index}'")
      assert_equal text, result.to_a.first[0]
    end
  end

  def test_text_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "text_null_test", :text)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT text_col FROM cassandra_c_test.test_text_types WHERE id = 'text_null_test'")
    assert_nil result.to_a.first[0]
  end

  def test_text_empty_string
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "text_empty_test", :text)
    statement.bind_by_index(1, "", :text)
    session.execute(statement)

    result = session.query("SELECT text_col FROM cassandra_c_test.test_text_types WHERE id = 'text_empty_test'")
    assert_equal "", result.to_a.first[0]
  end

  def test_text_automatic_inference
    # Test string automatic inference
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.test_text_types (id, text_col) VALUES (?, ?)", 2)

    statement.bind_by_index(0, "text_auto_test", :text)  # Explicit hint for ID
    statement.bind_by_index(1, "Auto inferred text")     # Should infer as text
    session.execute(statement)

    result = session.query("SELECT text_col FROM cassandra_c_test.test_text_types WHERE id = 'text_auto_test'")
    assert_instance_of String, result.to_a.first[0]
    assert_equal "Auto inferred text", result.to_a.first[0]
  end
end
