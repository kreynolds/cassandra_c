# frozen_string_literal: true

require "test_helper"

class TestBlobTypes < Minitest::Test
  def test_blob_binding_by_index
    # Test various binary data types
    binary_data = [
      "\x00\x01\x02\x03\x04\xFF".b,           # Basic binary sequence
      (+"Hello World").force_encoding("BINARY"),  # Text as binary
      "\x89PNG\r\n\x1a\n".b,                  # PNG file header
      "\xFF\xD8\xFF\xE0".b,                   # JPEG file header
      Random.bytes(1024),                      # Random binary data
      "".b                                     # Empty binary data
    ]

    binary_data.each_with_index do |data, index|
      prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
      statement = prepared.bind
      statement.bind_text_by_index(0, "blob_test_#{index}")
      statement.bind_blob_by_index(1, data)

      result = session.execute(statement)
      assert_kind_of CassandraC::Native::Result, result
    end
  end

  def test_blob_binding_by_name
    binary_data = "\x48\x65\x6C\x6C\x6F\x20\x57\x6F\x72\x6C\x64\x00\xFF".b

    prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (:id, :blob_data)")
    statement = prepared.bind
    statement.bind_text_by_name("id", "blob_by_name_test")
    statement.bind_blob_by_name("blob_data", binary_data)

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_blob_encoding_preservation
    original_data = "\x00\x01\x02\x03\xFF\xFE\xFD".b
    assert_equal Encoding::ASCII_8BIT, original_data.encoding

    prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_text_by_index(0, "encoding_test")
    statement.bind_blob_by_index(1, original_data)

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_large_blob_data
    # Test with larger binary data (64KB)
    large_data = Random.bytes(65536)
    assert_equal 65536, large_data.bytesize

    prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_text_by_index(0, "large_blob_test")
    statement.bind_blob_by_index(1, large_data)

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_blob_with_null_bytes
    data_with_nulls = "Before\x00Middle\x00\x00After\x00".b

    prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_text_by_index(0, "null_bytes_test")
    statement.bind_blob_by_index(1, data_with_nulls)

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_blob_null_values
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_text_by_index(0, "null_blob_test")
    statement.bind_blob_by_index(1, nil)

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_blob_empty_data
    empty_blob = "".b

    prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_text_by_index(0, "empty_blob_test")
    statement.bind_blob_by_index(1, empty_blob)

    result = session.execute(statement)
    assert_kind_of CassandraC::Native::Result, result
  end

  def test_blob_type_checking_for_non_strings
    prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_text_by_index(0, "type_check_test")

    # Test that non-string values raise TypeError
    [123, 45.67, true, false, [], {}].each do |non_string_value|
      error = assert_raises(TypeError) do
        statement.bind_blob_by_index(1, non_string_value)
      end

      assert_match(/wrong argument type/, error.message)
    end
  end

  def test_simulated_file_data
    # Simulate storing different file types as blobs
    file_data = {
      "pdf" => "%PDF-1.4\n1 0 obj\n".b,
      "zip" => "PK\x03\x04\x14\x00".b,
      "gif" => "GIF89a".b,
      "exe" => "MZ\x90\x00".b
    }

    file_data.each do |file_type, data|
      prepared = session.prepare("INSERT INTO cassandra_c_test.test_blob_types (id, blob_data) VALUES (?, ?)")
      statement = prepared.bind
      statement.bind_text_by_index(0, "file_#{file_type}")
      statement.bind_blob_by_index(1, data)

      result = session.execute(statement)
      assert_kind_of CassandraC::Native::Result, result
    end
  end
end
