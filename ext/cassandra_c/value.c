#include "cassandra_c.h"

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