#include "cassandra_c.h"
#include <string.h>
#include "ruby/encoding.h"

// Ruby classes for integer types (will be looked up at runtime)
static VALUE cTinyInt = Qnil;
static VALUE cSmallInt = Qnil;
static VALUE cInt = Qnil;
static VALUE cBigInt = Qnil;
static VALUE cVarInt = Qnil;

// Ruby classes for floating point types (will be looked up at runtime)
static VALUE cFloat = Qnil;
static VALUE cDouble = Qnil;
static VALUE cDecimal = Qnil;

// Forward declarations
static VALUE ruby_decimal_from_varint(const cass_byte_t* varint, size_t varint_size, cass_int32_t scale);
static void ruby_integer_to_varint_bytes(VALUE integer, cass_byte_t** varint_bytes, size_t* varint_size);
static void ruby_decimal_to_varint_bytes(VALUE decimal, cass_byte_t** varint_bytes, size_t* varint_size, cass_int32_t* scale);
static VALUE ruby_varint_bytes_to_integer(const cass_byte_t* varint, size_t varint_size);

// Helper function to initialize type class references
static void init_type_classes() {
    if (cTinyInt == Qnil) {
        VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
        VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
        cTinyInt = rb_const_get(mTypes, rb_intern("TinyInt"));
        cSmallInt = rb_const_get(mTypes, rb_intern("SmallInt"));
        cInt = rb_const_get(mTypes, rb_intern("Int"));
        cBigInt = rb_const_get(mTypes, rb_intern("BigInt"));
        cVarInt = rb_const_get(mTypes, rb_intern("VarInt"));
        cFloat = rb_const_get(mTypes, rb_intern("Float"));
        cDouble = rb_const_get(mTypes, rb_intern("Double"));
        cDecimal = rb_const_get(mTypes, rb_intern("Decimal"));
    }
}

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
        case T_FIXNUM:
        case T_BIGNUM: {
            init_type_classes();
            VALUE rb_class = rb_obj_class(rb_value);
            
            if (rb_class == cTinyInt) {
                cass_int8_t val = (cass_int8_t)NUM2INT(rb_value);
                return cass_statement_bind_int8(statement, index, val);
            } else if (rb_class == cSmallInt) {
                cass_int16_t val = (cass_int16_t)NUM2INT(rb_value);
                return cass_statement_bind_int16(statement, index, val);
            } else if (rb_class == cInt) {
                cass_int32_t val = (cass_int32_t)NUM2LONG(rb_value);
                return cass_statement_bind_int32(statement, index, val);
            } else if (rb_class == cBigInt) {
                cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
                return cass_statement_bind_int64(statement, index, val);
            } else if (rb_class == cVarInt) {
                // Convert to string for VARINT
                VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                const char* str = RSTRING_PTR(str_val);
                size_t len = RSTRING_LEN(str_val);
                return cass_statement_bind_string_n(statement, index, str, len);
            } else {
                // Default integer handling - use int32 for small values, int64 for large
                if (TYPE(rb_value) == T_FIXNUM) {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(rb_value);
                    return cass_statement_bind_int32(statement, index, val);
                } else {
                    cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
                    return cass_statement_bind_int64(statement, index, val);
                }
            }
        }
        case T_OBJECT: {
            // Check if it's a typed integer
            if (rb_respond_to(rb_value, rb_intern("cassandra_typed_integer?"))) {
                init_type_classes();
                VALUE rb_class = rb_obj_class(rb_value);
                VALUE int_val = rb_funcall(rb_value, rb_intern("to_i"), 0);
                
                if (rb_class == cTinyInt) {
                    cass_int8_t val = (cass_int8_t)NUM2INT(int_val);
                    return cass_statement_bind_int8(statement, index, val);
                } else if (rb_class == cSmallInt) {
                    cass_int16_t val = (cass_int16_t)NUM2INT(int_val);
                    return cass_statement_bind_int16(statement, index, val);
                } else if (rb_class == cInt) {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(int_val);
                    return cass_statement_bind_int32(statement, index, val);
                } else if (rb_class == cBigInt) {
                    cass_int64_t val = (cass_int64_t)NUM2LL(int_val);
                    return cass_statement_bind_int64(statement, index, val);
                } else if (rb_class == cVarInt) {
                    // Convert to string for VARINT
                    VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    return cass_statement_bind_string_n(statement, index, str, len);
                }
            }
            // Check if it's a typed float or double
            if (rb_respond_to(rb_value, rb_intern("cassandra_typed_float?")) || 
                rb_respond_to(rb_value, rb_intern("cassandra_typed_double?"))) {
                init_type_classes();
                VALUE rb_class = rb_obj_class(rb_value);
                VALUE float_val = rb_funcall(rb_value, rb_intern("to_f"), 0);
                
                if (rb_class == cFloat) {
                    cass_float_t val = (cass_float_t)NUM2DBL(float_val);
                    return cass_statement_bind_float(statement, index, val);
                } else if (rb_class == cDouble) {
                    cass_double_t val = NUM2DBL(float_val);
                    return cass_statement_bind_double(statement, index, val);
                }
            }
            // Check if it's a typed decimal
            if (rb_respond_to(rb_value, rb_intern("cassandra_typed_decimal?"))) {
                return ruby_value_to_cass_decimal(statement, index, rb_value);
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
        case T_FIXNUM:
        case T_BIGNUM: {
            init_type_classes();
            VALUE rb_class = rb_obj_class(rb_value);
            
            if (rb_class == cTinyInt) {
                cass_int8_t val = (cass_int8_t)NUM2INT(rb_value);
                return cass_statement_bind_int8_by_name(statement, name, val);
            } else if (rb_class == cSmallInt) {
                cass_int16_t val = (cass_int16_t)NUM2INT(rb_value);
                return cass_statement_bind_int16_by_name(statement, name, val);
            } else if (rb_class == cInt) {
                cass_int32_t val = (cass_int32_t)NUM2LONG(rb_value);
                return cass_statement_bind_int32_by_name(statement, name, val);
            } else if (rb_class == cBigInt) {
                cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
                return cass_statement_bind_int64_by_name(statement, name, val);
            } else if (rb_class == cVarInt) {
                // Convert to string for VARINT
                VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                const char* str = RSTRING_PTR(str_val);
                size_t len = RSTRING_LEN(str_val);
                return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
            } else {
                // Default integer handling - use int32 for small values, int64 for large
                if (TYPE(rb_value) == T_FIXNUM) {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(rb_value);
                    return cass_statement_bind_int32_by_name(statement, name, val);
                } else {
                    cass_int64_t val = (cass_int64_t)NUM2LL(rb_value);
                    return cass_statement_bind_int64_by_name(statement, name, val);
                }
            }
        }
        case T_OBJECT: {
            // Check if it's a typed integer
            if (rb_respond_to(rb_value, rb_intern("cassandra_typed_integer?"))) {
                init_type_classes();
                VALUE rb_class = rb_obj_class(rb_value);
                VALUE int_val = rb_funcall(rb_value, rb_intern("to_i"), 0);
                
                if (rb_class == cTinyInt) {
                    cass_int8_t val = (cass_int8_t)NUM2INT(int_val);
                    return cass_statement_bind_int8_by_name(statement, name, val);
                } else if (rb_class == cSmallInt) {
                    cass_int16_t val = (cass_int16_t)NUM2INT(int_val);
                    return cass_statement_bind_int16_by_name(statement, name, val);
                } else if (rb_class == cInt) {
                    cass_int32_t val = (cass_int32_t)NUM2LONG(int_val);
                    return cass_statement_bind_int32_by_name(statement, name, val);
                } else if (rb_class == cBigInt) {
                    cass_int64_t val = (cass_int64_t)NUM2LL(int_val);
                    return cass_statement_bind_int64_by_name(statement, name, val);
                } else if (rb_class == cVarInt) {
                    // Convert to string for VARINT
                    VALUE str_val = rb_funcall(rb_value, rb_intern("to_s"), 0);
                    const char* str = RSTRING_PTR(str_val);
                    size_t len = RSTRING_LEN(str_val);
                    return cass_statement_bind_string_by_name_n(statement, name, strlen(name), str, len);
                }
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
            break;
        }
        case CASS_VALUE_TYPE_TINY_INT: {
            cass_int8_t i8;
            cass_value_get_int8(value, &i8);
            init_type_classes();
            rb_value = rb_funcall(cTinyInt, rb_intern("new"), 1, INT2NUM(i8));
            break;
        }
        case CASS_VALUE_TYPE_SMALL_INT: {
            cass_int16_t i16;
            cass_value_get_int16(value, &i16);
            init_type_classes();
            rb_value = rb_funcall(cSmallInt, rb_intern("new"), 1, INT2NUM(i16));
            break;
        }
        case CASS_VALUE_TYPE_INT: {
            cass_int32_t i32;
            cass_value_get_int32(value, &i32);
            init_type_classes();
            rb_value = rb_funcall(cInt, rb_intern("new"), 1, LONG2NUM(i32));
            break;
        }
        case CASS_VALUE_TYPE_BIGINT:
        case CASS_VALUE_TYPE_COUNTER: {
            cass_int64_t i64;
            cass_value_get_int64(value, &i64);
            init_type_classes();
            rb_value = rb_funcall(cBigInt, rb_intern("new"), 1, LL2NUM(i64));
            break;
        }
        case CASS_VALUE_TYPE_VARINT: {
            // VARINT values can be retrieved as string for simplicity
            const char* text;
            size_t text_length;
            cass_value_get_string(value, &text, &text_length);
            VALUE str_val = rb_str_new(text, text_length);
            VALUE int_val = rb_funcall(str_val, rb_intern("to_i"), 0);
            init_type_classes();
            rb_value = rb_funcall(cVarInt, rb_intern("new"), 1, int_val);
            break;
        }
        case CASS_VALUE_TYPE_BOOLEAN: {
            cass_bool_t b;
            cass_value_get_bool(value, &b);
            rb_value = b ? Qtrue : Qfalse;
            break;
        }
        case CASS_VALUE_TYPE_DOUBLE: {
            init_type_classes();
            cass_double_t d;
            cass_value_get_double(value, &d);
            VALUE args[] = { rb_float_new(d) };
            rb_value = rb_class_new_instance(1, args, cDouble);
            break;
        }
        case CASS_VALUE_TYPE_FLOAT: {
            init_type_classes();
            cass_float_t f;
            cass_value_get_float(value, &f);
            VALUE args[] = { rb_float_new(f) };
            rb_value = rb_class_new_instance(1, args, cFloat);
            break;
        }
        case CASS_VALUE_TYPE_DECIMAL: {
            init_type_classes();
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
            rb_value = rb_str_new_cstr(uuid_str);
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
    
    // Handle CassandraC::Types::Float objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cFloat = rb_const_get(mTypes, rb_intern("Float"));
    
    if (rb_obj_is_kind_of(rb_value, cFloat)) {
        VALUE float_value = rb_funcall(rb_value, rb_intern("to_f"), 0);
        float_val = (cass_float_t)NUM2DBL(float_value);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
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
    
    // Handle CassandraC::Types::Float objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cFloat = rb_const_get(mTypes, rb_intern("Float"));
    
    if (rb_obj_is_kind_of(rb_value, cFloat)) {
        VALUE float_value = rb_funcall(rb_value, rb_intern("to_f"), 0);
        float_val = (cass_float_t)NUM2DBL(float_value);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
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
    
    // Handle CassandraC::Types::Double objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cDouble = rb_const_get(mTypes, rb_intern("Double"));
    
    if (rb_obj_is_kind_of(rb_value, cDouble)) {
        VALUE double_value = rb_funcall(rb_value, rb_intern("to_f"), 0);
        double_val = NUM2DBL(double_value);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
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
    
    // Handle CassandraC::Types::Double objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cDouble = rb_const_get(mTypes, rb_intern("Double"));
    
    if (rb_obj_is_kind_of(rb_value, cDouble)) {
        VALUE double_value = rb_funcall(rb_value, rb_intern("to_f"), 0);
        double_val = NUM2DBL(double_value);
    } else if (FIXNUM_P(rb_value) || TYPE(rb_value) == T_BIGNUM || TYPE(rb_value) == T_FLOAT) {
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

// Helper function to convert Ruby BigDecimal to varint bytes for DECIMAL
static void ruby_decimal_to_varint_bytes(VALUE decimal, cass_byte_t** varint_bytes, size_t* varint_size, cass_int32_t* scale) {
    // Get the unscaled value and scale from the Decimal object
    VALUE unscaled_val = rb_funcall(decimal, rb_intern("unscaled_value"), 0);
    VALUE scale_val = rb_funcall(decimal, rb_intern("scale"), 0);
    *scale = NUM2INT(scale_val);
    
    // Convert the unscaled integer to varint bytes
    ruby_integer_to_varint_bytes(unscaled_val, varint_bytes, varint_size);
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
    
    // Create CassandraC::Types::Decimal instance
    init_type_classes();
    VALUE args[] = { big_decimal, INT2NUM(scale) };
    return rb_class_new_instance(2, args, cDecimal);
}

// Type-specific binding functions for decimal (arbitrary precision)
CassError ruby_value_to_cass_decimal(CassStatement* statement, size_t index, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null(statement, index);
    }
    
    cass_int32_t scale;
    
    // Handle CassandraC::Types::Decimal objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cDecimal = rb_const_get(mTypes, rb_intern("Decimal"));
    
    if (rb_obj_is_kind_of(rb_value, cDecimal)) {
        cass_byte_t* varint_bytes;
        size_t varint_size;
        ruby_decimal_to_varint_bytes(rb_value, &varint_bytes, &varint_size, &scale);
        
        CassError error = cass_statement_bind_decimal(statement, index, varint_bytes, varint_size, scale);
        
        // Free the allocated varint bytes
        free(varint_bytes);
        
        return error;
    } else {
        // Try to create a Decimal from the value
        VALUE decimal = rb_funcall(rb_value, rb_intern("to_cassandra_decimal"), 0);
        return ruby_value_to_cass_decimal(statement, index, decimal);
    }
}

CassError ruby_value_to_cass_decimal_by_name(CassStatement* statement, const char* name, VALUE rb_value) {
    if (NIL_P(rb_value)) {
        return cass_statement_bind_null_by_name(statement, name);
    }
    
    cass_int32_t scale;
    
    // Handle CassandraC::Types::Decimal objects
    VALUE mCassandraC = rb_const_get(rb_cObject, rb_intern("CassandraC"));
    VALUE mTypes = rb_const_get(mCassandraC, rb_intern("Types"));
    VALUE cDecimal = rb_const_get(mTypes, rb_intern("Decimal"));
    
    if (rb_obj_is_kind_of(rb_value, cDecimal)) {
        cass_byte_t* varint_bytes;
        size_t varint_size;
        ruby_decimal_to_varint_bytes(rb_value, &varint_bytes, &varint_size, &scale);
        
        CassError error = cass_statement_bind_decimal_by_name(statement, name, varint_bytes, varint_size, scale);
        
        // Free the allocated varint bytes
        free(varint_bytes);
        
        return error;
    } else {
        // Try to create a Decimal from the value
        VALUE decimal = rb_funcall(rb_value, rb_intern("to_cassandra_decimal"), 0);
        return ruby_value_to_cass_decimal_by_name(statement, name, decimal);
    }
}