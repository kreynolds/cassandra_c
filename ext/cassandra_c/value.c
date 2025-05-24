#include "cassandra_c.h"
#include <string.h>

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
            break;
        }
        case CASS_VALUE_TYPE_INT: {
            cass_int32_t i32;
            cass_value_get_int32(value, &i32);
            rb_value = INT2NUM(i32);
            break;
        }
        case CASS_VALUE_TYPE_BIGINT:
        case CASS_VALUE_TYPE_COUNTER: {
            cass_int64_t i64;
            cass_value_get_int64(value, &i64);
            rb_value = LL2NUM(i64);
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
        case CASS_VALUE_TYPE_UUID: {
            CassUuid uuid;
            cass_value_get_uuid(value, &uuid);
            char uuid_str[CASS_UUID_STRING_LENGTH];
            cass_uuid_string(uuid, uuid_str);
            rb_value = rb_str_new_cstr(uuid_str);
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