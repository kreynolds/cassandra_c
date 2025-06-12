#include "cassandra_c.h"
#include <string.h>
#include "ruby/encoding.h"

// No wrapper classes needed - using native Ruby types


// Forward declarations
static VALUE ruby_decimal_from_varint(const cass_byte_t* varint, size_t varint_size, cass_int32_t scale);
static void ruby_integer_to_varint_bytes(VALUE integer, cass_byte_t** varint_bytes, size_t* varint_size);
static VALUE ruby_varint_bytes_to_integer(const cass_byte_t* varint, size_t varint_size);

// No initialization needed for native Ruby types

// Helper function to validate that a string contains only ASCII characters
static int is_ascii_string(const char* str, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if ((unsigned char)str[i] > 127) {
            return 0; // Non-ASCII character found
        }
    }
    return 1; // All characters are ASCII
}

// Helper function to bind a Ruby value to a CassStatement at a given index
CassError ruby_value_to_cass_statement(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    switch (TYPE(rb_value)) {
        case T_STRING: {
            const char* str = RSTRING_PTR(rb_value);
            size_t len = RSTRING_LEN(rb_value);
            return cass_statement_bind_string_n(statement, index, str, len);
        }
        case T_FLOAT: {
            double val = NUM2DBL(rb_value);
            return cass_statement_bind_double(statement, index, val);
        }
        case T_TRUE:
            return cass_statement_bind_bool(statement, index, cass_true);
        case T_FALSE:
            return cass_statement_bind_bool(statement, index, cass_false);
        case T_ARRAY: {
            // Handle plain Ruby arrays as lists
            return ruby_value_to_cass_list(statement, index, rb_value);
        }
        case T_FIXNUM:
        case T_BIGNUM: {
            // Check if this is actually a BigDecimal (which can report as T_BIGNUM)
            VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
            if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
                return ruby_value_to_cass_decimal(statement, index, rb_value);
            }
            // Default integer handling - use bigint (int64) for all integers
            cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
            return cass_statement_bind_int64(statement, index, val);
        }
        case T_HASH: {
            // Handle Ruby Hash as map
            return ruby_value_to_cass_map(statement, index, rb_value);
        }
        case T_DATA: {
            // Check if it's a BigDecimal (BigDecimal objects are T_DATA, not T_OBJECT)
            VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
            if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
                return ruby_value_to_cass_decimal(statement, index, rb_value);
            }
            // Fall through to default case for other T_DATA objects
        }
        case T_OBJECT: {
            // Check if it's a Ruby Set object
            VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
            if (rb_obj_is_kind_of(rb_value, set_class)) {
                return ruby_value_to_cass_set(statement, index, rb_value);
            }
            // Check if it's a BigDecimal (backup check, though BigDecimal should be T_DATA)
            VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
            if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
                return ruby_value_to_cass_decimal(statement, index, rb_value);
            }
            // Check if it's a Time object
            VALUE rb_cTime = rb_const_get(rb_cObject, rb_intern("Time"));
            if (rb_obj_is_kind_of(rb_value, rb_cTime)) {
                // Convert Time to timestamp (bigint - milliseconds since epoch)
                VALUE to_f_val = rb_funcall(rb_value, rb_intern("to_f"), 0);
                double time_f = NUM2DBL(to_f_val);
                cass_int64_t timestamp = (cass_int64_t)(time_f * 1000); // Convert to milliseconds
                return cass_statement_bind_int64(statement, index, timestamp);
            }
            // Check if it's a Date object (only if Date class is defined)
            VALUE rb_cDate = Qnil;
            if (rb_const_defined(rb_cObject, rb_intern("Date"))) {
                rb_cDate = rb_const_get(rb_cObject, rb_intern("Date"));
                if (rb_obj_is_kind_of(rb_value, rb_cDate)) {
                    // Convert Date to string representation
                    VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    return cass_statement_bind_string_n(statement, index, str, len);
                }
            }
            // Check if it's a TimeUuid object - for now, check by class name
            VALUE rb_class = rb_obj_class(rb_value);
            VALUE class_name = rb_funcall(rb_class, rb_intern("name"), 0);
            if (rb_str_equal(class_name, rb_str_new_cstr("CassandraC::Types::TimeUuid"))) {
                // Convert TimeUuid to string and bind as timeuuid
                VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                return ruby_value_to_cass_timeuuid(statement, index, str_val);
            }
            // Fall through to default case for other objects
        }
        default: {
            // Try to convert to string as fallback
            VALUE str_val = rb_obj_as_string(rb_value);
            const char* str = RSTRING_PTR(str_val);
            size_t len = RSTRING_LEN(str_val);
            return cass_statement_bind_string_n(statement, index, str, len);
        }
    }
}

// Helper function to bind a Ruby value to a CassStatement by name
CassError ruby_value_to_cass_statement_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    switch (TYPE(rb_value)) {
        case T_STRING: {
            const char* str = RSTRING_PTR(rb_value);
            size_t len = RSTRING_LEN(rb_value);
            return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
        }
        case T_FLOAT: {
            double val = NUM2DBL(rb_value);
            return cass_statement_bind_double_by_name(statement, name, val);
        }
        case T_TRUE:
            return cass_statement_bind_bool_by_name(statement, name, cass_true);
        case T_FALSE:
            return cass_statement_bind_bool_by_name(statement, name, cass_false);
        case T_ARRAY: {
            // Handle plain Ruby arrays as lists
            return ruby_value_to_cass_list_by_name(statement, name, rb_value);
        }
        case T_FIXNUM:
        case T_BIGNUM: {
            // Check if this is actually a BigDecimal (which can report as T_BIGNUM)
            VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
            if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
                return ruby_value_to_cass_decimal_by_name(statement, name, rb_value);
            }
            // Default integer handling - use bigint (int64) for all integers
            cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
            return cass_statement_bind_int64_by_name(statement, name, val);
        }
        case T_HASH: {
            // Handle Ruby Hash as map
            return ruby_value_to_cass_map_by_name(statement, name, rb_value);
        }
        case T_DATA: {
            // Check if it's a BigDecimal (BigDecimal objects are T_DATA, not T_OBJECT)
            VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
            if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
                return ruby_value_to_cass_decimal_by_name(statement, name, rb_value);
            }
            // Fall through to default case for other T_DATA objects
        }
        case T_OBJECT: {
            // Check if it's a Ruby Set object
            VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
            if (rb_obj_is_kind_of(rb_value, set_class)) {
                return ruby_value_to_cass_set_by_name(statement, name, rb_value);
            }
            // Check if it's a BigDecimal (backup check, though BigDecimal should be T_DATA)
            VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
            if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
                return ruby_value_to_cass_decimal_by_name(statement, name, rb_value);
            }
            // Check if it's a Time object
            VALUE rb_cTime = rb_const_get(rb_cObject, rb_intern("Time"));
            if (rb_obj_is_kind_of(rb_value, rb_cTime)) {
                // Convert Time to timestamp (bigint - milliseconds since epoch)
                VALUE to_f_val = rb_funcall(rb_value, rb_intern("to_f"), 0);
                double time_f = NUM2DBL(to_f_val);
                cass_int64_t timestamp = (cass_int64_t)(time_f * 1000); // Convert to milliseconds
                return cass_statement_bind_int64_by_name(statement, name, timestamp);
            }
            // Check if it's a Date object (only if Date class is defined)
            VALUE rb_cDate = Qnil;
            if (rb_const_defined(rb_cObject, rb_intern("Date"))) {
                rb_cDate = rb_const_get(rb_cObject, rb_intern("Date"));
                if (rb_obj_is_kind_of(rb_value, rb_cDate)) {
                    // Convert Date to string representation
                    VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
                }
            }
            // Check if it's a TimeUuid object - for now, check by class name
            VALUE rb_class = rb_obj_class(rb_value);
            VALUE class_name = rb_funcall(rb_class, rb_intern("name"), 0);
            if (rb_str_equal(class_name, rb_str_new_cstr("CassandraC::Types::TimeUuid"))) {
                // Convert TimeUuid to string and bind as timeuuid
                VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                return ruby_value_to_cass_timeuuid_by_name(statement, name, str_val);
            }
            // Fall through to default case for other objects
        }
        default: {
            // Try to convert to string as fallback
            VALUE str_val = rb_obj_as_string(rb_value);
            const char* str = RSTRING_PTR(str_val);
            size_t len = RSTRING_LEN(str_val);
            return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
        }
    }
}

// Helper function to convert a CassValue to a Ruby object
VALUE cass_value_to_ruby(const CassValue* value) {
    if (value == NULL || cass_value_is_null(value)) {
        return Qnil;
    }
    
    CassValueType type = cass_value_type(value);
    VALUE rb_value = Qnil;
    
    switch (type) {
        case CASS_VALUE_TYPE_ASCII:
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR: {
            const char* text;
            size_t text_length;
            cass_value_get_string(value, &text, &text_length);
            rb_value = rb_str_new(text, text_length);
            rb_enc_associate(rb_value, rb_utf8_encoding());
            break;
        }
        case CASS_VALUE_TYPE_TINY_INT: {
            cass_int8_t i8;
            cass_value_get_int8(value, &i8);
            rb_value = INT2NUM(i8);
            break;
        }
        case CASS_VALUE_TYPE_SMALL_INT: {
            cass_int16_t i16;
            cass_value_get_int16(value, &i16);
            rb_value = INT2NUM(i16);
            break;
        }
        case CASS_VALUE_TYPE_INT: {
            cass_int32_t i32;
            cass_value_get_int32(value, &i32);
            rb_value = LONG2NUM(i32);
            break;
        }
        case CASS_VALUE_TYPE_BIGINT:
        case CASS_VALUE_TYPE_COUNTER: {
            cass_int64_t i64;
            cass_value_get_int64(value, &i64);
            rb_value = LL2NUM(i64);
            break;
        }
        case CASS_VALUE_TYPE_VARINT: {
            // VARINT values can be retrieved as string for simplicity
            const char* text;
            size_t text_length;
            cass_value_get_string(value, &text, &text_length);
            VALUE str_val = rb_str_new(text, text_length);
            rb_value = rb_funcall(str_val, rb_intern("to_i"), 0);
            break;
        }
        case CASS_VALUE_TYPE_BOOLEAN: {
            cass_bool_t b;
            cass_value_get_bool(value, &b);
            rb_value = b ? Qtrue : Qfalse;
            break;
        }
        case CASS_VALUE_TYPE_DOUBLE: {
            cass_double_t d;
            cass_value_get_double(value, &d);
            rb_value = rb_float_new(d);
            break;
        }
        case CASS_VALUE_TYPE_FLOAT: {
            cass_float_t f;
            cass_value_get_float(value, &f);
            rb_value = rb_float_new(f);
            break;
        }
        case CASS_VALUE_TYPE_DECIMAL: {
            const cass_byte_t* varint;
            size_t varint_size;
            cass_int32_t scale;
            cass_value_get_decimal(value, &varint, &varint_size, &scale);
            
            // Convert varint bytes to Ruby BigDecimal
            rb_value = ruby_decimal_from_varint(varint, varint_size, scale);
            break;
        }
        case CASS_VALUE_TYPE_UUID: {
            CassUuid uuid;
            cass_value_get_uuid(value, &uuid);
            char uuid_str[CASS_UUID_STRING_LENGTH];
            cass_uuid_string(uuid, uuid_str);
            
            // Return as regular Ruby string
            rb_value = rb_str_new_cstr(uuid_str);
            break;
        }
        case CASS_VALUE_TYPE_TIMEUUID: {
            CassUuid timeuuid;
            cass_value_get_uuid(value, &timeuuid);
            char timeuuid_str[CASS_UUID_STRING_LENGTH];
            cass_uuid_string(timeuuid, timeuuid_str);
            
            // Return as TimeUuid wrapper object
            VALUE rb_str = rb_str_new_cstr(timeuuid_str);
            VALUE types_module = rb_const_get(mCassandraC, rb_intern("Types"));
            VALUE timeuuid_class = rb_const_get(types_module, rb_intern("TimeUuid"));
            rb_value = rb_funcall(timeuuid_class, rb_intern("new"), 1, rb_str);
            break;
        }
        case CASS_VALUE_TYPE_BLOB: {
            const cass_byte_t* bytes;
            size_t bytes_length;
            cass_value_get_bytes(value, &bytes, &bytes_length);
            rb_value = rb_str_new((const char*)bytes, bytes_length);
            // Set encoding to ASCII-8BIT (binary) for blob data
            rb_enc_associate(rb_value, rb_ascii8bit_encoding());
            break;
        }
        case CASS_VALUE_TYPE_INET: {
            CassInet inet;
            cass_value_get_inet(value, &inet);
            char inet_str[CASS_INET_STRING_LENGTH];
            cass_inet_string(inet, inet_str);
            rb_value = rb_str_new_cstr(inet_str);
            break;
        }
        case CASS_VALUE_TYPE_LIST: {
            // Create a Ruby array to hold the list elements
            VALUE rb_array = rb_ary_new();
            
            // Get an iterator for the collection
            CassIterator* iterator = cass_iterator_from_collection(value);
            
            // Iterate through each element and convert to Ruby
            while (cass_iterator_next(iterator)) {
                const CassValue* element = cass_iterator_get_value(iterator);
                VALUE rb_element = cass_value_to_ruby(element);
                rb_ary_push(rb_array, rb_element);
            }
            
            cass_iterator_free(iterator);
            
            // Return plain Ruby array
            rb_value = rb_array;
            break;
        }
        case CASS_VALUE_TYPE_SET: {
            // Create a Ruby array to hold the set elements, then convert to Set
            VALUE rb_array = rb_ary_new();
            
            // Get an iterator for the collection
            CassIterator* iterator = cass_iterator_from_collection(value);
            
            // Iterate through each element and convert to Ruby
            while (cass_iterator_next(iterator)) {
                const CassValue* element = cass_iterator_get_value(iterator);
                VALUE rb_element = cass_value_to_ruby(element);
                rb_ary_push(rb_array, rb_element);
            }
            
            cass_iterator_free(iterator);
            
            // Convert array to Ruby Set
            VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
            rb_value = rb_funcall(set_class, rb_intern("new"), 1, rb_array);
            break;
        }
        case CASS_VALUE_TYPE_MAP: {
            // Create a Ruby hash to hold the map elements
            VALUE rb_hash = rb_hash_new();
            
            // Get an iterator for the map
            CassIterator* iterator = cass_iterator_from_map(value);
            
            // Iterate through each key-value pair and convert to Ruby
            while (cass_iterator_next(iterator)) {
                const CassValue* key = cass_iterator_get_map_key(iterator);
                const CassValue* val = cass_iterator_get_map_value(iterator);
                VALUE rb_key = cass_value_to_ruby(key);
                VALUE rb_val = cass_value_to_ruby(val);
                rb_hash_aset(rb_hash, rb_key, rb_val);
            }
            
            cass_iterator_free(iterator);
            
            // Return Ruby hash
            rb_value = rb_hash;
            break;
        }
        // Add other data types as needed
        default:
            rb_value = rb_str_new_cstr("[unsupported type]");
            break;
    }
    
    return rb_value;
}

// Type-specific binding functions for text/varchar (UTF-8 strings)
CassError ruby_string_to_cass_text(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    Check_Type(rb_value, T_STRING);
    
    const char* str = RSTRING_PTR(rb_value);
    size_t len = RSTRING_LEN(rb_value);
    
    // Text/varchar accepts any UTF-8 string
    return cass_statement_bind_string_n(statement, index, str, len);
}

CassError ruby_string_to_cass_text_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    Check_Type(rb_value, T_STRING);
    
    const char* str = RSTRING_PTR(rb_value);
    size_t len = RSTRING_LEN(rb_value);
    
    // Text/varchar accepts any UTF-8 string
    return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
}

// Type-specific binding functions for ASCII strings
CassError ruby_string_to_cass_ascii(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    Check_Type(rb_value, T_STRING);
    
    const char* str = RSTRING_PTR(rb_value);
    size_t len = RSTRING_LEN(rb_value);
    
    // ASCII type requires validation - only 7-bit ASCII characters allowed
    if (!is_ascii_string(str, len)) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_string_n(statement, index, str, len);
}

CassError ruby_string_to_cass_ascii_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    Check_Type(rb_value, T_STRING);
    
    const char* str = RSTRING_PTR(rb_value);
    size_t len = RSTRING_LEN(rb_value);
    
    // ASCII type requires validation - only 7-bit ASCII characters allowed
    if (!is_ascii_string(str, len)) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
}

// Type-specific binding functions for blob (binary data)
CassError ruby_string_to_cass_blob(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    Check_Type(rb_value, T_STRING);
    
    const char* data = RSTRING_PTR(rb_value);
    size_t len = RSTRING_LEN(rb_value);
    
    // Blob accepts any binary data
    return cass_statement_bind_bytes(statement, index, (const cass_byte_t*)data, len);
}

CassError ruby_string_to_cass_blob_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    Check_Type(rb_value, T_STRING);
    
    const char* data = RSTRING_PTR(rb_value);
    size_t len = RSTRING_LEN(rb_value);
    
    // Blob accepts any binary data
    return cass_statement_bind_bytes_by_name(statement, name, (const cass_byte_t*)data, len);
}

// Helper function to convert Ruby value to CassInet
static CassError ruby_value_to_cass_inet_value(VALUE rb_value, CassInet* inet) {
    const char* ip_str;
    VALUE str_value;
    
    // Handle different input types
    if (TYPE(rb_value) == T_STRING) {
        // Direct string input
        str_value = rb_value;
        ip_str = RSTRING_PTR(str_value);
    } else {
        // Check if it's an IPAddr object
        VALUE ipaddr_class = rb_const_get(rb_cObject, rb_intern("IPAddr"));
        if (rb_obj_is_kind_of(rb_value, ipaddr_class)) {
            // Convert IPAddr to string
            str_value = rb_funcall(rb_value, rb_intern("to_s"), 0);
            ip_str = RSTRING_PTR(str_value);
        } else {
            // Try to convert to string as fallback
            str_value = rb_obj_as_string(rb_value);
            ip_str = RSTRING_PTR(str_value);
        }
    }
    
    // Parse the IP address string into CassInet
    return cass_inet_from_string(ip_str, inet);
}

// Type-specific binding functions for inet (IP addresses)
CassError ruby_value_to_cass_inet(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    CassInet inet;
    CassError error = ruby_value_to_cass_inet_value(rb_value, &inet);
    if (error != CASS_OK) {
        return error;
    }
    
    return cass_statement_bind_inet(statement, index, inet);
}

CassError ruby_value_to_cass_inet_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    CassInet inet;
    CassError error = ruby_value_to_cass_inet_value(rb_value, &inet);
    if (error != CASS_OK) {
        return error;
    }
    
    return cass_statement_bind_inet_by_name(statement, name, inet);
}

// Type-specific binding functions for float (32-bit IEEE 754)
CassError ruby_value_to_cass_float(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    cass_float_t float_val;
    
    // Handle Ruby numeric types
    if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
        float_val = (cass_float_t)NUM2DBL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_float(statement, index, float_val);
}

CassError ruby_value_to_cass_float_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    cass_float_t float_val;
    
    // Handle Ruby numeric types
    if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
        float_val = (cass_float_t)NUM2DBL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_float_by_name(statement, name, float_val);
}

// Type-specific binding functions for double (64-bit IEEE 754)
CassError ruby_value_to_cass_double(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }

    cass_double_t double_val;

    // Handle Ruby numeric types
    if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
        double_val = NUM2DBL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }

    return cass_statement_bind_double(statement, index, double_val);
}

CassError ruby_value_to_cass_double_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }

    cass_double_t double_val;

    // Handle Ruby numeric types
    if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
        double_val = NUM2DBL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }

    return cass_statement_bind_double_by_name(statement, name, double_val);
}

// Helper function to encode Ruby integer to varint bytes
static void ruby_integer_to_varint_bytes(VALUE integer, cass_byte_t** varint_bytes, size_t* varint_size) {
    // For simplicity and to avoid complex varint encoding issues, 
    // we'll use a more direct approach that works with Ruby's integer representation
    
    // Check if zero
    VALUE zero = INT2NUM(0);
    if (rb_funcall(integer, rb_intern("=="), 1, zero) == Qtrue) {
        *varint_bytes = (cass_byte_t*)malloc(1);
        (*varint_bytes)[0] = 0;
        *varint_size = 1;
        return;
    }
    
    // Get absolute value for magnitude calculation
    VALUE abs_val = rb_funcall(integer, rb_intern("abs"), 0);
    int is_negative = (rb_funcall(integer, rb_intern("<"), 1, zero) == Qtrue);
    
    // Calculate minimum number of bytes needed
    // Using bit_length method if available, otherwise estimate
    VALUE bit_length;
    if (rb_respond_to(abs_val, rb_intern("bit_length"))) {
        bit_length = rb_funcall(abs_val, rb_intern("bit_length"), 0);
    } else {
        // Estimate: log2(value) + 1
        VALUE str_val = rb_funcall(abs_val, rb_intern("to_s"), 0);
        size_t str_len = RSTRING_LEN(str_val);
        bit_length = INT2NUM((int)(str_len * 4)); // Conservative estimate
    }
    
    size_t byte_length = (NUM2INT(bit_length) + 7) / 8;
    if (byte_length == 0) byte_length = 1;
    
    // For negative numbers, we may need an extra byte for sign extension
    if (is_negative) byte_length++;
    
    unsigned char* bytes = (unsigned char*)malloc(byte_length);
    memset(bytes, 0, byte_length);
    
    // Extract bytes using Ruby's bit shift operations
    VALUE temp = abs_val;
    size_t actual_bytes = 0;
    
    for (size_t i = 0; i < byte_length && rb_funcall(temp, rb_intern(">"), 1, zero) == Qtrue; i++) {
        VALUE byte_val = rb_funcall(temp, rb_intern("&"), 1, INT2NUM(0xFF));
        bytes[byte_length - 1 - i] = NUM2INT(byte_val);
        temp = rb_funcall(temp, rb_intern(">>"), 1, INT2NUM(8));
        actual_bytes = i + 1;
    }
    
    if (actual_bytes == 0) {
        actual_bytes = 1;
    }
    
    // Adjust buffer size to actual bytes needed
    if (actual_bytes < byte_length) {
        memmove(bytes, bytes + byte_length - actual_bytes, actual_bytes);
        bytes = (unsigned char*)realloc(bytes, actual_bytes);
        byte_length = actual_bytes;
    }
    
    // Apply two's complement for negative numbers
    if (is_negative) {
        // Invert all bits
        for (size_t i = 0; i < byte_length; i++) {
            bytes[i] = ~bytes[i];
        }
        
        // Add 1
        int carry = 1;
        for (int i = (int)byte_length - 1; i >= 0 && carry; i--) {
            int sum = bytes[i] + carry;
            bytes[i] = sum & 0xFF;
            carry = sum >> 8;
        }
        
        // Ensure sign extension for negative numbers
        if (carry || (bytes[0] & 0x80) == 0) {
            bytes = (unsigned char*)realloc(bytes, byte_length + 1);
            memmove(bytes + 1, bytes, byte_length);
            bytes[0] = 0xFF;
            byte_length++;
        }
    }
    
    *varint_bytes = (cass_byte_t*)bytes;
    *varint_size = byte_length;
}


// Helper function to convert varint bytes to Ruby integer
static VALUE ruby_varint_bytes_to_integer(const cass_byte_t* varint, size_t varint_size) {
    if (varint_size == 0) {
        return INT2NUM(0);
    }
    
    // Check if the number is negative (MSB of first byte is set)
    int is_negative = (varint[0] & 0x80) != 0;
    
    // Build the number from bytes (big-endian)
    VALUE result = INT2NUM(0);
    VALUE byte_multiplier = INT2NUM(1);
    
    // Process bytes from right to left (least significant first for our calculation)
    for (int i = (int)varint_size - 1; i >= 0; i--) {
        unsigned char byte_val = varint[i];
        
        // If negative, we need to handle two's complement
        if (is_negative) {
            // For two's complement, we first invert all bits, then add 1
            // We'll do this at the end for the complete number
        }
        
        VALUE byte_contribution = rb_funcall(INT2NUM(byte_val), rb_intern("*"), 1, byte_multiplier);
        result = rb_funcall(result, rb_intern("+"), 1, byte_contribution);
        byte_multiplier = rb_funcall(byte_multiplier, rb_intern("*"), 1, INT2NUM(256));
    }
    
    // Handle two's complement for negative numbers
    if (is_negative) {
        // Calculate 2^(bits) - result for two's complement
        VALUE max_val = rb_funcall(INT2NUM(2), rb_intern("**"), 1, INT2NUM((int)(varint_size * 8)));
        result = rb_funcall(result, rb_intern("-"), 1, max_val);
    }
    
    return result;
}

// Helper function to convert varint bytes to Ruby Decimal
static VALUE ruby_decimal_from_varint(const cass_byte_t* varint, size_t varint_size, cass_int32_t scale) {
    // Convert varint bytes to Ruby integer
    VALUE unscaled = ruby_varint_bytes_to_integer(varint, varint_size);
    
    // Create BigDecimal from unscaled value and scale  
    VALUE decimal_str = rb_funcall(unscaled, rb_intern("to_s"), 0);
    VALUE big_decimal = rb_funcall(rb_mKernel, rb_intern("BigDecimal"), 1, decimal_str);
    
    // Apply the scale (divide by 10^scale)
    if (scale > 0) {
        VALUE divisor = rb_funcall(INT2NUM(10), rb_intern("**"), 1, INT2NUM(scale));
        big_decimal = rb_funcall(big_decimal, rb_intern("/"), 1, divisor);
    }
    
    // Return BigDecimal directly
    return big_decimal;
}

// Type-specific binding functions for decimal (arbitrary precision)
CassError ruby_value_to_cass_decimal(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }

    cass_int32_t scale = 0;

    // Handle BigDecimal objects directly
    VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
    if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
        // Convert BigDecimal to string, then parse for scale
        VALUE decimal_str = rb_funcall(rb_value, rb_intern("to_s"), 1, rb_str_new_cstr("F"));
        const char* str = RSTRING_PTR(decimal_str);
        
        // Find the decimal point to determine scale
        const char* decimal_point = strchr(str, '.');
        if (decimal_point != NULL) {
            scale = (cass_int32_t)strlen(decimal_point + 1);
        }
        
        // Convert to unscaled integer
        VALUE multiplier = rb_funcall(INT2NUM(10), rb_intern("**"), 1, INT2NUM(scale));
        VALUE unscaled = rb_funcall(rb_value, rb_intern("*"), 1, multiplier);
        unscaled = rb_funcall(unscaled, rb_intern("to_i"), 0);
        
        cass_byte_t* varint_bytes;
        size_t varint_size;
        ruby_integer_to_varint_bytes(unscaled, &varint_bytes, &varint_size);

        CassError error = cass_statement_bind_decimal(statement, index, varint_bytes, varint_size, scale);

        // Free the allocated varint bytes
        free(varint_bytes);

        return error;
    } else {
        // Try to convert to BigDecimal first
        VALUE big_decimal = rb_funcall(rb_mKernel, rb_intern("BigDecimal"), 1, rb_obj_as_string(rb_value));
        return ruby_value_to_cass_decimal(statement, index, big_decimal);
    }
}

CassError ruby_value_to_cass_decimal_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }

    cass_int32_t scale = 0;

    // Handle BigDecimal objects directly
    VALUE rb_cBigDecimal = rb_const_get(rb_cObject, rb_intern("BigDecimal"));
    if (rb_obj_is_kind_of(rb_value, rb_cBigDecimal)) {
        // Convert BigDecimal to string, then parse for scale
        VALUE decimal_str = rb_funcall(rb_value, rb_intern("to_s"), 1, rb_str_new_cstr("F"));
        const char* str = RSTRING_PTR(decimal_str);
        
        // Find the decimal point to determine scale
        const char* decimal_point = strchr(str, '.');
        if (decimal_point != NULL) {
            scale = (cass_int32_t)strlen(decimal_point + 1);
        }
        
        // Convert to unscaled integer
        VALUE multiplier = rb_funcall(INT2NUM(10), rb_intern("**"), 1, INT2NUM(scale));
        VALUE unscaled = rb_funcall(rb_value, rb_intern("*"), 1, multiplier);
        unscaled = rb_funcall(unscaled, rb_intern("to_i"), 0);
        
        cass_byte_t* varint_bytes;
        size_t varint_size;
        ruby_integer_to_varint_bytes(unscaled, &varint_bytes, &varint_size);

        CassError error = cass_statement_bind_decimal_by_name(statement, name, varint_bytes, varint_size, scale);

        // Free the allocated varint bytes
        free(varint_bytes);

        return error;
    } else {
        // Try to convert to BigDecimal first
        VALUE big_decimal = rb_funcall(rb_mKernel, rb_intern("BigDecimal"), 1, rb_obj_as_string(rb_value));
        return ruby_value_to_cass_decimal_by_name(statement, name, big_decimal);
    }
}

// Type-specific binding functions for UUID
CassError ruby_value_to_cass_uuid(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }

    // Handle regular Ruby strings as UUIDs
    if (TYPE(rb_value) != T_STRING) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }

    const char* uuid_cstr = RSTRING_PTR(rb_value);
    CassUuid uuid;
    CassError error = cass_uuid_from_string(uuid_cstr, &uuid);
    if (error != CASS_OK) {
        return error;
    }

    return cass_statement_bind_uuid(statement, index, uuid);
}

CassError ruby_value_to_cass_uuid_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }

    // Handle regular Ruby strings as UUIDs
    if (TYPE(rb_value) != T_STRING) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }

    const char* uuid_cstr = RSTRING_PTR(rb_value);
    CassUuid uuid;
    CassError error = cass_uuid_from_string(uuid_cstr, &uuid);
    if (error != CASS_OK) {
        return error;
    }

    return cass_statement_bind_uuid_by_name(statement, name, uuid);
}

// Type-specific binding functions for TimeUUID
CassError ruby_value_to_cass_timeuuid(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }

    // Handle regular Ruby strings as TimeUUIDs
    if (TYPE(rb_value) != T_STRING) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }

    const char* timeuuid_cstr = RSTRING_PTR(rb_value);
    CassUuid timeuuid;
    CassError error = cass_uuid_from_string(timeuuid_cstr, &timeuuid);
    if (error != CASS_OK) {
        return error;
    }

    return cass_statement_bind_uuid(statement, index, timeuuid);
}

CassError ruby_value_to_cass_timeuuid_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }

    // Handle regular Ruby strings as TimeUUIDs
    if (TYPE(rb_value) != T_STRING) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }

    const char* timeuuid_cstr = RSTRING_PTR(rb_value);
    CassUuid timeuuid;
    CassError error = cass_uuid_from_string(timeuuid_cstr, &timeuuid);
    if (error != CASS_OK) {
        return error;
    }

    return cass_statement_bind_uuid_by_name(statement, name, timeuuid);
}

// Helper function to convert Ruby array to CassCollection
static CassError ruby_array_to_cass_collection(VALUE rb_array, CassCollection** collection) {
    if (TYPE(rb_array) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    long array_length = RARRAY_LEN(rb_array);
    
    // Create a new list collection (note: we'll need the element type later for better type safety)
    *collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, array_length);
    if (*collection == NULL) {
        return CASS_ERROR_LIB_INTERNAL_ERROR;
    }
    
    // Add each element to the collection
    for (long i = 0; i < array_length; i++) {
        VALUE element = rb_ary_entry(rb_array, i);
        CassError error = CASS_OK;
        
        if (NIL_P(element)) {
            error = cass_collection_append_string(*collection, NULL);
        } else {
            switch (TYPE(element)) {
                case T_STRING: {
                    const char* str = RSTRING_PTR(element);
                    size_t len = RSTRING_LEN(element);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
                case T_FIXNUM: {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(element);
                    error = cass_collection_append_int32(*collection, val);
                    break;
                }
                case T_BIGNUM: {
                    cass_int64_t val = (cass_int64_t)NUM2LL(element);
                    error = cass_collection_append_int64(*collection, val);
                    break;
                }
                case T_FLOAT: {
                    cass_double_t val = NUM2DBL(element);
                    error = cass_collection_append_double(*collection, val);
                    break;
                }
                case T_TRUE:
                    error = cass_collection_append_bool(*collection, cass_true);
                    break;
                case T_FALSE:
                    error = cass_collection_append_bool(*collection, cass_false);
                    break;
                default: {
                    // Convert to string as fallback
                    VALUE str_val = rb_obj_as_string(element);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
            }
        }
        
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
    }
    
    return CASS_OK;
}

CassError ruby_value_to_cass_list(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    if (TYPE(rb_value) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_array_to_cass_collection(rb_value, &collection);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection(statement, index, collection);
    cass_collection_free(collection);
    
    return error;
}

CassError ruby_value_to_cass_list_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    if (TYPE(rb_value) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_array_to_cass_collection(rb_value, &collection);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection_by_name(statement, name, collection);
    cass_collection_free(collection);
    
    return error;
}

// Helper function to convert Ruby Set to CassCollection
static CassError ruby_set_to_cass_collection(VALUE rb_set, CassCollection** collection) {
    // Convert Ruby Set to Array first
    VALUE rb_array = rb_funcall(rb_set, rb_intern("to_a"), 0);
    
    if (TYPE(rb_array) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    long array_length = RARRAY_LEN(rb_array);
    
    // Create a new set collection
    *collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, array_length);
    if (*collection == NULL) {
        return CASS_ERROR_LIB_INTERNAL_ERROR;
    }
    
    // Add each element to the collection
    for (long i = 0; i < array_length; i++) {
        VALUE element = rb_ary_entry(rb_array, i);
        CassError error = CASS_OK;
        
        if (NIL_P(element)) {
            error = cass_collection_append_string(*collection, NULL);
        } else {
            switch (TYPE(element)) {
                case T_STRING: {
                    const char* str = RSTRING_PTR(element);
                    size_t len = RSTRING_LEN(element);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
                case T_FIXNUM: {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(element);
                    error = cass_collection_append_int32(*collection, val);
                    break;
                }
                case T_BIGNUM: {
                    cass_int64_t val = (cass_int64_t)NUM2LL(element);
                    error = cass_collection_append_int64(*collection, val);
                    break;
                }
                case T_FLOAT: {
                    cass_double_t val = NUM2DBL(element);
                    error = cass_collection_append_double(*collection, val);
                    break;
                }
                case T_TRUE:
                    error = cass_collection_append_bool(*collection, cass_true);
                    break;
                case T_FALSE:
                    error = cass_collection_append_bool(*collection, cass_false);
                    break;
                default: {
                    // Convert to string as fallback
                    VALUE str_val = rb_obj_as_string(element);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
            }
        }
        
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
    }
    
    return CASS_OK;
}

CassError ruby_value_to_cass_set(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    // Check if it's a Ruby Set
    VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
    if (!rb_obj_is_kind_of(rb_value, set_class)) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_set_to_cass_collection(rb_value, &collection);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection(statement, index, collection);
    cass_collection_free(collection);
    
    return error;
}

CassError ruby_value_to_cass_set_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    // Check if it's a Ruby Set
    VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
    if (!rb_obj_is_kind_of(rb_value, set_class)) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_set_to_cass_collection(rb_value, &collection);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection_by_name(statement, name, collection);
    cass_collection_free(collection);
    
    return error;
}

// Helper function to convert Ruby Hash to CassCollection
static CassError ruby_hash_to_cass_collection(VALUE rb_hash, CassCollection** collection) {
    if (TYPE(rb_hash) != T_HASH) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    long hash_size = RHASH_SIZE(rb_hash);
    
    // Create a new map collection
    *collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, hash_size);
    if (*collection == NULL) {
        return CASS_ERROR_LIB_INTERNAL_ERROR;
    }
    
    // Get keys and values arrays
    VALUE keys = rb_funcall(rb_hash, rb_intern("keys"), 0);
    VALUE values = rb_funcall(rb_hash, rb_intern("values"), 0);
    
    // Add each key-value pair to the collection
    for (long i = 0; i < hash_size; i++) {
        VALUE key = rb_ary_entry(keys, i);
        VALUE value = rb_ary_entry(values, i);
        CassError error = CASS_OK;
        
        // Add key to collection
        if (NIL_P(key)) {
            error = cass_collection_append_string(*collection, NULL);
        } else {
            switch (TYPE(key)) {
                case T_STRING: {
                    const char* str = RSTRING_PTR(key);
                    size_t len = RSTRING_LEN(key);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
                case T_FIXNUM: {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(key);
                    error = cass_collection_append_int32(*collection, val);
                    break;
                }
                case T_BIGNUM: {
                    cass_int64_t val = (cass_int64_t)NUM2LL(key);
                    error = cass_collection_append_int64(*collection, val);
                    break;
                }
                case T_FLOAT: {
                    cass_double_t val = NUM2DBL(key);
                    error = cass_collection_append_double(*collection, val);
                    break;
                }
                case T_TRUE:
                    error = cass_collection_append_bool(*collection, cass_true);
                    break;
                case T_FALSE:
                    error = cass_collection_append_bool(*collection, cass_false);
                    break;
                default: {
                    // Convert to string as fallback
                    VALUE str_val = rb_obj_as_string(key);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
            }
        }
        
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
        
        // Add value to collection
        if (NIL_P(value)) {
            error = cass_collection_append_string(*collection, NULL);
        } else {
            switch (TYPE(value)) {
                case T_STRING: {
                    const char* str = RSTRING_PTR(value);
                    size_t len = RSTRING_LEN(value);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
                case T_FIXNUM: {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(value);
                    error = cass_collection_append_int32(*collection, val);
                    break;
                }
                case T_BIGNUM: {
                    cass_int64_t val = (cass_int64_t)NUM2LL(value);
                    error = cass_collection_append_int64(*collection, val);
                    break;
                }
                case T_FLOAT: {
                    cass_double_t val = NUM2DBL(value);
                    error = cass_collection_append_double(*collection, val);
                    break;
                }
                case T_TRUE:
                    error = cass_collection_append_bool(*collection, cass_true);
                    break;
                case T_FALSE:
                    error = cass_collection_append_bool(*collection, cass_false);
                    break;
                default: {
                    // Convert to string as fallback
                    VALUE str_val = rb_obj_as_string(value);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    error = cass_collection_append_string_n(*collection, str, len);
                    break;
                }
            }
        }
        
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
    }
    
    return CASS_OK;
}

CassError ruby_value_to_cass_map(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    // Check if it's a Ruby Hash
    if (TYPE(rb_value) != T_HASH) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_hash_to_cass_collection(rb_value, &collection);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection(statement, index, collection);
    cass_collection_free(collection);
    
    return error;
}

CassError ruby_value_to_cass_map_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    // Check if it's a Ruby Hash
    if (TYPE(rb_value) != T_HASH) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_hash_to_cass_collection(rb_value, &collection);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection_by_name(statement, name, collection);
    cass_collection_free(collection);
    
    return error;
}

// ============================================================================
// Type-hinted Collection Binding Functions
// ============================================================================

// Helper function to convert Ruby type symbol to CassValueType for element binding
static CassValueType ruby_symbol_to_cass_value_type(VALUE type_symbol) {
    if (NIL_P(type_symbol)) {
        return CASS_VALUE_TYPE_UNKNOWN;
    }
    
    Check_Type(type_symbol, T_SYMBOL);
    ID type_id = SYM2ID(type_symbol);
    
    if (type_id == rb_intern("tinyint")) return CASS_VALUE_TYPE_TINY_INT;
    if (type_id == rb_intern("smallint")) return CASS_VALUE_TYPE_SMALL_INT;
    if (type_id == rb_intern("int")) return CASS_VALUE_TYPE_INT;
    if (type_id == rb_intern("bigint")) return CASS_VALUE_TYPE_BIGINT;
    if (type_id == rb_intern("varint")) return CASS_VALUE_TYPE_VARINT;
    if (type_id == rb_intern("float")) return CASS_VALUE_TYPE_FLOAT;
    if (type_id == rb_intern("double")) return CASS_VALUE_TYPE_DOUBLE;
    if (type_id == rb_intern("decimal")) return CASS_VALUE_TYPE_DECIMAL;
    if (type_id == rb_intern("text")) return CASS_VALUE_TYPE_TEXT;
    if (type_id == rb_intern("varchar")) return CASS_VALUE_TYPE_VARCHAR;
    if (type_id == rb_intern("ascii")) return CASS_VALUE_TYPE_ASCII;
    if (type_id == rb_intern("blob")) return CASS_VALUE_TYPE_BLOB;
    if (type_id == rb_intern("boolean")) return CASS_VALUE_TYPE_BOOLEAN;
    if (type_id == rb_intern("uuid")) return CASS_VALUE_TYPE_UUID;
    if (type_id == rb_intern("timeuuid")) return CASS_VALUE_TYPE_TIMEUUID;
    if (type_id == rb_intern("inet")) return CASS_VALUE_TYPE_INET;
    if (type_id == rb_intern("date")) return CASS_VALUE_TYPE_DATE;
    if (type_id == rb_intern("time")) return CASS_VALUE_TYPE_TIME;
    if (type_id == rb_intern("timestamp")) return CASS_VALUE_TYPE_TIMESTAMP;
    
    return CASS_VALUE_TYPE_UNKNOWN;
}

// Helper function to bind element to collection with specific type
static CassError bind_element_to_collection_with_type(CassCollection* collection, VALUE element, CassValueType type) {
    if (NIL_P(element)) {
        return cass_collection_append_string(collection, NULL);
    }
    
    switch (type) {
        case CASS_VALUE_TYPE_TINY_INT: {
            cass_int8_t val = (cass_int8_t)NUM2INT(element);
            return cass_collection_append_int8(collection, val);
        }
        case CASS_VALUE_TYPE_SMALL_INT: {
            cass_int16_t val = (cass_int16_t)NUM2INT(element);
            return cass_collection_append_int16(collection, val);
        }
        case CASS_VALUE_TYPE_INT: {
            cass_int32_t val = (cass_int32_t)NUM2LONG(element);
            return cass_collection_append_int32(collection, val);
        }
        case CASS_VALUE_TYPE_BIGINT: {
            cass_int64_t val = (cass_int64_t)NUM2LL(element);
            return cass_collection_append_int64(collection, val);
        }
        case CASS_VALUE_TYPE_VARINT: {
            // Convert to string representation for varint
            VALUE str_val = rb_obj_as_string(element);
            const char* str = RSTRING_PTR(str_val);
            size_t len = RSTRING_LEN(str_val);
            return cass_collection_append_string_n(collection, str, len);
        }
        case CASS_VALUE_TYPE_FLOAT: {
            cass_float_t val = (cass_float_t)NUM2DBL(element);
            return cass_collection_append_float(collection, val);
        }
        case CASS_VALUE_TYPE_DOUBLE: {
            cass_double_t val = NUM2DBL(element);
            return cass_collection_append_double(collection, val);
        }
        case CASS_VALUE_TYPE_BOOLEAN: {
            cass_bool_t val = RTEST(element) ? cass_true : cass_false;
            return cass_collection_append_bool(collection, val);
        }
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR:
        case CASS_VALUE_TYPE_ASCII: {
            const char* str = RSTRING_PTR(element);
            size_t len = RSTRING_LEN(element);
            return cass_collection_append_string_n(collection, str, len);
        }
        case CASS_VALUE_TYPE_BLOB: {
            const char* data = RSTRING_PTR(element);
            size_t len = RSTRING_LEN(element);
            return cass_collection_append_bytes(collection, (const cass_byte_t*)data, len);
        }
        default: {
            // Fallback to string conversion
            VALUE str_val = rb_obj_as_string(element);
            const char* str = RSTRING_PTR(str_val);
            size_t len = RSTRING_LEN(str_val);
            return cass_collection_append_string_n(collection, str, len);
        }
    }
}

// Helper function to convert Ruby array to CassCollection with specific element type
static CassError ruby_array_to_cass_collection_with_type(VALUE rb_array, CassCollection** collection, VALUE element_type) {
    if (TYPE(rb_array) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    long array_length = RARRAY_LEN(rb_array);
    CassValueType type = ruby_symbol_to_cass_value_type(element_type);
    
    // Create a new list collection
    *collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, array_length);
    if (*collection == NULL) {
        return CASS_ERROR_LIB_INTERNAL_ERROR;
    }
    
    // Add each element to the collection with the specified type
    for (long i = 0; i < array_length; i++) {
        VALUE element = rb_ary_entry(rb_array, i);
        CassError error = bind_element_to_collection_with_type(*collection, element, type);
        
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
    }
    
    return CASS_OK;
}

// Helper function to convert Ruby Set to CassCollection with specific element type
static CassError ruby_set_to_cass_collection_with_type(VALUE rb_set, CassCollection** collection, VALUE element_type) {
    // Convert Ruby Set to Array first
    VALUE rb_array = rb_funcall(rb_set, rb_intern("to_a"), 0);
    
    if (TYPE(rb_array) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    long array_length = RARRAY_LEN(rb_array);
    CassValueType type = ruby_symbol_to_cass_value_type(element_type);
    
    // Create a new set collection
    *collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, array_length);
    if (*collection == NULL) {
        return CASS_ERROR_LIB_INTERNAL_ERROR;
    }
    
    // Add each element to the collection with the specified type
    for (long i = 0; i < array_length; i++) {
        VALUE element = rb_ary_entry(rb_array, i);
        CassError error = bind_element_to_collection_with_type(*collection, element, type);
        
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
    }
    
    return CASS_OK;
}

// Helper function to convert Ruby Hash to CassCollection with specific key and value types
static CassError ruby_hash_to_cass_collection_with_type(VALUE rb_hash, CassCollection** collection, VALUE key_type, VALUE value_type) {
    if (TYPE(rb_hash) != T_HASH) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    long hash_size = RHASH_SIZE(rb_hash);
    CassValueType k_type = ruby_symbol_to_cass_value_type(key_type);
    CassValueType v_type = ruby_symbol_to_cass_value_type(value_type);
    
    // Create a new map collection
    *collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, hash_size);
    if (*collection == NULL) {
        return CASS_ERROR_LIB_INTERNAL_ERROR;
    }
    
    // Get keys and values arrays
    VALUE keys = rb_funcall(rb_hash, rb_intern("keys"), 0);
    VALUE values = rb_funcall(rb_hash, rb_intern("values"), 0);
    
    // Add each key-value pair to the collection with the specified types
    for (long i = 0; i < hash_size; i++) {
        VALUE key = rb_ary_entry(keys, i);
        VALUE value = rb_ary_entry(values, i);
        
        // Add key to collection
        CassError error = bind_element_to_collection_with_type(*collection, key, k_type);
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
        
        // Add value to collection
        error = bind_element_to_collection_with_type(*collection, value, v_type);
        if (error != CASS_OK) {
            cass_collection_free(*collection);
            *collection = NULL;
            return error;
        }
    }
    
    return CASS_OK;
}

// Type-hinted list binding functions
CassError ruby_value_to_cass_list_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE element_type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    if (TYPE(rb_value) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_array_to_cass_collection_with_type(rb_value, &collection, element_type);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection(statement, index, collection);
    cass_collection_free(collection);
    
    return error;
}

CassError ruby_value_to_cass_list_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE element_type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    if (TYPE(rb_value) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_array_to_cass_collection_with_type(rb_value, &collection, element_type);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection_by_name(statement, name, collection);
    cass_collection_free(collection);
    
    return error;
}

// Type-hinted set binding functions  
CassError ruby_value_to_cass_set_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE element_type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    // Check if it's a Ruby Set or Array
    VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
    if (!rb_obj_is_kind_of(rb_value, set_class) && TYPE(rb_value) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error;
    
    if (rb_obj_is_kind_of(rb_value, set_class)) {
        error = ruby_set_to_cass_collection_with_type(rb_value, &collection, element_type);
    } else {
        // Convert array to set
        VALUE set_from_array = rb_funcall(rb_const_get(rb_cObject, rb_intern("Set")), rb_intern("new"), 1, rb_value);
        error = ruby_set_to_cass_collection_with_type(set_from_array, &collection, element_type);
    }
    
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection(statement, index, collection);
    cass_collection_free(collection);
    
    return error;
}

CassError ruby_value_to_cass_set_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE element_type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    // Check if it's a Ruby Set or Array
    VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
    if (!rb_obj_is_kind_of(rb_value, set_class) && TYPE(rb_value) != T_ARRAY) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error;
    
    if (rb_obj_is_kind_of(rb_value, set_class)) {
        error = ruby_set_to_cass_collection_with_type(rb_value, &collection, element_type);
    } else {
        // Convert array to set
        VALUE set_from_array = rb_funcall(rb_const_get(rb_cObject, rb_intern("Set")), rb_intern("new"), 1, rb_value);
        error = ruby_set_to_cass_collection_with_type(set_from_array, &collection, element_type);
    }
    
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection_by_name(statement, name, collection);
    cass_collection_free(collection);
    
    return error;
}

// Type-hinted map binding functions
CassError ruby_value_to_cass_map_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE key_type, VALUE value_type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    if (TYPE(rb_value) != T_HASH) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_hash_to_cass_collection_with_type(rb_value, &collection, key_type, value_type);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection(statement, index, collection);
    cass_collection_free(collection);
    
    return error;
}

CassError ruby_value_to_cass_map_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE key_type, VALUE value_type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    if (TYPE(rb_value) != T_HASH) {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    CassCollection* collection;
    CassError error = ruby_hash_to_cass_collection_with_type(rb_value, &collection, key_type, value_type);
    if (error != CASS_OK) {
        return error;
    }
    
    error = cass_statement_bind_collection_by_name(statement, name, collection);
    cass_collection_free(collection);
    
    return error;
}

// ============================================================================
// Type-hinted Scalar Value Binding Functions
// ============================================================================

// Helper function to bind a value with a specific type hint
static CassError bind_value_with_type_hint(CassStatement* statement, size_t index, VALUE rb_value, CassValueType type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    switch (type) {
        case CASS_VALUE_TYPE_TINY_INT: {
            cass_int8_t val = (cass_int8_t)NUM2INT(rb_value);
            return cass_statement_bind_int8(statement, index, val);
        }
        case CASS_VALUE_TYPE_SMALL_INT: {
            cass_int16_t val = (cass_int16_t)NUM2INT(rb_value);
            return cass_statement_bind_int16(statement, index, val);
        }
        case CASS_VALUE_TYPE_INT: {
            cass_int32_t val = (cass_int32_t)NUM2LONG(rb_value);
            return cass_statement_bind_int32(statement, index, val);
        }
        case CASS_VALUE_TYPE_BIGINT: {
            cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
            return cass_statement_bind_int64(statement, index, val);
        }
        case CASS_VALUE_TYPE_VARINT: {
            // Convert to string representation for varint
            VALUE str_val = rb_obj_as_string(rb_value);
            const char* str = RSTRING_PTR(str_val);
            size_t len = RSTRING_LEN(str_val);
            return cass_statement_bind_string_n(statement, index, str, len);
        }
        case CASS_VALUE_TYPE_FLOAT: {
            cass_float_t val = (cass_float_t)NUM2DBL(rb_value);
            return cass_statement_bind_float(statement, index, val);
        }
        case CASS_VALUE_TYPE_DOUBLE: {
            cass_double_t val = NUM2DBL(rb_value);
            return cass_statement_bind_double(statement, index, val);
        }
        case CASS_VALUE_TYPE_BOOLEAN: {
            cass_bool_t val = RTEST(rb_value) ? cass_true : cass_false;
            return cass_statement_bind_bool(statement, index, val);
        }
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR: {
            const char* str = RSTRING_PTR(rb_value);
            size_t len = RSTRING_LEN(rb_value);
            return cass_statement_bind_string_n(statement, index, str, len);
        }
        case CASS_VALUE_TYPE_ASCII: {
            return ruby_string_to_cass_ascii(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_BLOB: {
            return ruby_string_to_cass_blob(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_INET: {
            return ruby_value_to_cass_inet(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_DECIMAL: {
            return ruby_value_to_cass_decimal(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_UUID: {
            return ruby_value_to_cass_uuid(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_TIMEUUID: {
            return ruby_value_to_cass_timeuuid(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_DATE: {
            return ruby_value_to_cass_date(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_TIME: {
            return ruby_value_to_cass_time(statement, index, rb_value);
        }
        case CASS_VALUE_TYPE_TIMESTAMP: {
            return ruby_value_to_cass_timestamp(statement, index, rb_value);
        }
        default: {
            // Fall back to default binding behavior
            return ruby_value_to_cass_statement(statement, index, rb_value);
        }
    }
}

// Helper function to bind a value with a specific type hint by name
static CassError bind_value_with_type_hint_by_name(CassStatement* statement, const char* name, VALUE rb_value, CassValueType type) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    switch (type) {
        case CASS_VALUE_TYPE_TINY_INT: {
            cass_int8_t val = (cass_int8_t)NUM2INT(rb_value);
            return cass_statement_bind_int8_by_name(statement, name, val);
        }
        case CASS_VALUE_TYPE_SMALL_INT: {
            cass_int16_t val = (cass_int16_t)NUM2INT(rb_value);
            return cass_statement_bind_int16_by_name(statement, name, val);
        }
        case CASS_VALUE_TYPE_INT: {
            cass_int32_t val = (cass_int32_t)NUM2LONG(rb_value);
            return cass_statement_bind_int32_by_name(statement, name, val);
        }
        case CASS_VALUE_TYPE_BIGINT: {
            cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
            return cass_statement_bind_int64_by_name(statement, name, val);
        }
        case CASS_VALUE_TYPE_VARINT: {
            // Convert to string representation for varint
            VALUE str_val = rb_obj_as_string(rb_value);
            const char* str = RSTRING_PTR(str_val);
            size_t len = RSTRING_LEN(str_val);
            return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
        }
        case CASS_VALUE_TYPE_FLOAT: {
            cass_float_t val = (cass_float_t)NUM2DBL(rb_value);
            return cass_statement_bind_float_by_name(statement, name, val);
        }
        case CASS_VALUE_TYPE_DOUBLE: {
            cass_double_t val = NUM2DBL(rb_value);
            return cass_statement_bind_double_by_name(statement, name, val);
        }
        case CASS_VALUE_TYPE_BOOLEAN: {
            cass_bool_t val = RTEST(rb_value) ? cass_true : cass_false;
            return cass_statement_bind_bool_by_name(statement, name, val);
        }
        case CASS_VALUE_TYPE_TEXT:
        case CASS_VALUE_TYPE_VARCHAR: {
            const char* str = RSTRING_PTR(rb_value);
            size_t len = RSTRING_LEN(rb_value);
            return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
        }
        case CASS_VALUE_TYPE_ASCII: {
            return ruby_string_to_cass_ascii_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_BLOB: {
            return ruby_string_to_cass_blob_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_INET: {
            return ruby_value_to_cass_inet_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_DECIMAL: {
            return ruby_value_to_cass_decimal_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_UUID: {
            return ruby_value_to_cass_uuid_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_TIMEUUID: {
            return ruby_value_to_cass_timeuuid_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_DATE: {
            return ruby_value_to_cass_date_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_TIME: {
            return ruby_value_to_cass_time_by_name(statement, name, rb_value);
        }
        case CASS_VALUE_TYPE_TIMESTAMP: {
            return ruby_value_to_cass_timestamp_by_name(statement, name, rb_value);
        }
        default: {
            // Fall back to default binding behavior
            return ruby_value_to_cass_statement_by_name(statement, name, rb_value);
        }
    }
}

// Type-hinted binding function for scalar values by index
CassError ruby_value_to_cass_statement_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE type_hint) {
    // Handle collection types
    if (TYPE(rb_value) == T_ARRAY) {
        return ruby_value_to_cass_list_with_type(statement, index, rb_value, type_hint);
    }
    
    VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
    if (rb_obj_is_kind_of(rb_value, set_class)) {
        return ruby_value_to_cass_set_with_type(statement, index, rb_value, type_hint);
    }
    
    if (TYPE(rb_value) == T_HASH) {
        // For maps, we need separate key and value type hints
        // For now, fall back to default behavior
        return ruby_value_to_cass_statement(statement, index, rb_value);
    }
    
    // Handle scalar types with type hint
    CassValueType type = ruby_symbol_to_cass_value_type(type_hint);
    if (type == CASS_VALUE_TYPE_UNKNOWN) {
        // Invalid type hint, fall back to default behavior
        return ruby_value_to_cass_statement(statement, index, rb_value);
    }
    
    return bind_value_with_type_hint(statement, index, rb_value, type);
}

// Type-hinted binding function for scalar values by name
CassError ruby_value_to_cass_statement_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE type_hint) {
    // Handle collection types
    if (TYPE(rb_value) == T_ARRAY) {
        return ruby_value_to_cass_list_with_type_by_name(statement, name, rb_value, type_hint);
    }
    
    VALUE set_class = rb_const_get(rb_cObject, rb_intern("Set"));
    if (rb_obj_is_kind_of(rb_value, set_class)) {
        return ruby_value_to_cass_set_with_type_by_name(statement, name, rb_value, type_hint);
    }
    
    if (TYPE(rb_value) == T_HASH) {
        // For maps, we need separate key and value type hints
        // For now, fall back to default behavior
        return ruby_value_to_cass_statement_by_name(statement, name, rb_value);
    }
    
    // Handle scalar types with type hint
    CassValueType type = ruby_symbol_to_cass_value_type(type_hint);
    if (type == CASS_VALUE_TYPE_UNKNOWN) {
        // Invalid type hint, fall back to default behavior
        return ruby_value_to_cass_statement_by_name(statement, name, rb_value);
    }
    
    return bind_value_with_type_hint_by_name(statement, name, rb_value, type);
}

// ============================================================================
// Date/Time Binding Functions  
// ============================================================================

// Type-specific binding functions for date (days since Unix epoch)
CassError ruby_value_to_cass_date(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    cass_uint32_t date_days;
    
    // Handle Ruby Date objects directly
    VALUE date_class = rb_const_get(rb_cObject, rb_intern("Date"));
    if (rb_obj_is_kind_of(rb_value, date_class)) {
        // Convert Ruby Date to days since Unix epoch
        VALUE epoch_date = rb_funcall(date_class, rb_intern("new"), 3, INT2NUM(1970), INT2NUM(1), INT2NUM(1));
        VALUE days_diff = rb_funcall(rb_value, rb_intern("-"), 1, epoch_date);
        date_days = (cass_uint32_t)NUM2UINT(rb_funcall(days_diff, rb_intern("to_i"), 0));
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM) {
        date_days = (cass_uint32_t)NUM2UINT(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_uint32(statement, index, date_days);
}

CassError ruby_value_to_cass_date_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    cass_uint32_t date_days;
    
    // Handle Ruby Date objects directly
    VALUE date_class = rb_const_get(rb_cObject, rb_intern("Date"));
    if (rb_obj_is_kind_of(rb_value, date_class)) {
        // Convert Ruby Date to days since Unix epoch
        VALUE epoch_date = rb_funcall(date_class, rb_intern("new"), 3, INT2NUM(1970), INT2NUM(1), INT2NUM(1));
        VALUE days_diff = rb_funcall(rb_value, rb_intern("-"), 1, epoch_date);
        date_days = (cass_uint32_t)NUM2UINT(rb_funcall(days_diff, rb_intern("to_i"), 0));
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM) {
        date_days = (cass_uint32_t)NUM2UINT(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_uint32_by_name(statement, name, date_days);
}

// Type-specific binding functions for time (nanoseconds since midnight)
CassError ruby_value_to_cass_time(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    cass_int64_t time_nanos;
    
    // Handle CassandraC::Types::Time objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cTime = rb_const_get(mTypes, rb_intern("Time"));
    
    if (rb_obj_is_kind_of(rb_value, cTime)) {
        VALUE nanos_value = rb_funcall(rb_value, rb_intern("nanoseconds_since_midnight"), 0);
        time_nanos = (cass_int64_t)NUM2LL(nanos_value);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM) {
        time_nanos = (cass_int64_t)NUM2LL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_int64(statement, index, time_nanos);
}

CassError ruby_value_to_cass_time_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    cass_int64_t time_nanos;
    
    // Handle CassandraC::Types::Time objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cTime = rb_const_get(mTypes, rb_intern("Time"));
    
    if (rb_obj_is_kind_of(rb_value, cTime)) {
        VALUE nanos_value = rb_funcall(rb_value, rb_intern("nanoseconds_since_midnight"), 0);
        time_nanos = (cass_int64_t)NUM2LL(nanos_value);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM) {
        time_nanos = (cass_int64_t)NUM2LL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_int64_by_name(statement, name, time_nanos);
}

// Type-specific binding functions for timestamp (milliseconds since Unix epoch)
CassError ruby_value_to_cass_timestamp(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    cass_int64_t timestamp_millis;
    
    // Handle Ruby Time objects directly
    VALUE time_class = rb_const_get(rb_cObject, rb_intern("Time"));
    if (rb_obj_is_kind_of(rb_value, time_class)) {
        // Convert Ruby Time to milliseconds since Unix epoch
        VALUE time_float = rb_funcall(rb_value, rb_intern("to_f"), 0);
        double time_seconds = NUM2DBL(time_float);
        timestamp_millis = (cass_int64_t)(time_seconds * 1000.0);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM) {
        timestamp_millis = (cass_int64_t)NUM2LL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_int64(statement, index, timestamp_millis);
}

CassError ruby_value_to_cass_timestamp_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    cass_int64_t timestamp_millis;
    
    // Handle Ruby Time objects directly
    VALUE time_class = rb_const_get(rb_cObject, rb_intern("Time"));
    if (rb_obj_is_kind_of(rb_value, time_class)) {
        // Convert Ruby Time to milliseconds since Unix epoch
        VALUE time_float = rb_funcall(rb_value, rb_intern("to_f"), 0);
        double time_seconds = NUM2DBL(time_float);
        timestamp_millis = (cass_int64_t)(time_seconds * 1000.0);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM) {
        timestamp_millis = (cass_int64_t)NUM2LL(rb_value);
    } else {
        return CASS_ERROR_LIB_INVALID_VALUE_TYPE;
    }
    
    return cass_statement_bind_int64_by_name(statement, name, timestamp_millis);
}