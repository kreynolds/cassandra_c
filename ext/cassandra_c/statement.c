#include "cassandra_c.h"

// Memory management for Statement
static void rb_statement_free(void* ptr) {
    StatementWrapper* wrapper = (StatementWrapper*)ptr;
    if (wrapper->statement != NULL) {
        cass_statement_free(wrapper->statement);
    }
    xfree(wrapper);
}

// Define the Ruby data type for Statement
const rb_data_type_t statement_type = {
    .wrap_struct_name = "CassStatement",
    .function = {
        .dmark = NULL,
        .dfree = rb_statement_free,
        .dsize = NULL,
    },
    .data = NULL,
    .flags = RUBY_TYPED_FREE_IMMEDIATELY
};

// Create a new Statement object
VALUE statement_new(CassStatement* statement) {
    StatementWrapper* wrapper = ALLOC(StatementWrapper);
    wrapper->statement = statement;
    VALUE rb_statement = TypedData_Wrap_Struct(cCassStatement, &statement_type, wrapper);
    return rb_statement;
}

// Allocation function for Statement
static VALUE rb_statement_allocate(VALUE klass) {
    StatementWrapper* wrapper = ALLOC(StatementWrapper);
    wrapper->statement = NULL; // Will be set in initialize
    return TypedData_Wrap_Struct(klass, &statement_type, wrapper);
}

// Initialize method for Statement
static VALUE rb_statement_initialize(int argc, VALUE* argv, VALUE self) {
    VALUE query, param_count;
    rb_scan_args(argc, argv, "11", &query, &param_count);

    Check_Type(query, T_STRING);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    // Free any existing statement
    if (wrapper->statement != NULL) {
        cass_statement_free(wrapper->statement);
    }
    
    if (NIL_P(param_count)) {
        // Create statement with default parameter count (0)
        wrapper->statement = cass_statement_new(StringValueCStr(query), 0);
    } else {
        // Create statement with specified parameter count
        wrapper->statement = cass_statement_new(StringValueCStr(query), NUM2UINT(param_count));
    }
    
    if (!wrapper->statement) {
        rb_raise(rb_eCassandraError, "Failed to create statement");
    }
    
    return self;
}


// Set consistency for this statement
static VALUE rb_statement_set_consistency(VALUE self, VALUE consistency) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    CassConsistency consistency_value = ruby_value_to_consistency(consistency);
    
    CassError error = cass_statement_set_consistency(wrapper->statement, consistency_value);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to set consistency level: %s", cass_error_desc(error));
    }
    
    return self;
}

// Bind a value by index with optional type hint
static VALUE rb_statement_bind_by_index(int argc, VALUE* argv, VALUE self) {
    VALUE index, value, type_hint;
    rb_scan_args(argc, argv, "21", &index, &value, &type_hint);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error;
    
    if (NIL_P(type_hint)) {
        // Use default binding logic without type hint
        error = ruby_value_to_cass_statement(wrapper->statement, param_index, value);
    } else {
        // Use type-hinted binding
        error = ruby_value_to_cass_statement_with_type(wrapper->statement, param_index, value, type_hint);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a value by name with optional type hint
static VALUE rb_statement_bind_by_name(int argc, VALUE* argv, VALUE self) {
    VALUE name, value, type_hint;
    rb_scan_args(argc, argv, "21", &name, &value, &type_hint);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error;
    
    if (NIL_P(type_hint)) {
        // Use default binding logic without type hint
        error = ruby_value_to_cass_statement_by_name(wrapper->statement, param_name, value);
    } else {
        // Use type-hinted binding
        error = ruby_value_to_cass_statement_with_type_by_name(wrapper->statement, param_name, value, type_hint);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a text/varchar value by index (UTF-8 strings)
static VALUE rb_statement_bind_text_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_string_to_cass_text(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind text parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a text/varchar value by name (UTF-8 strings)
static VALUE rb_statement_bind_text_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_string_to_cass_text_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind text parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind an ASCII value by index (ASCII-only strings)
static VALUE rb_statement_bind_ascii_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_string_to_cass_ascii(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        if (error == CASS_ERROR_LIB_INVALID_VALUE_TYPE) {
            rb_raise(rb_eCassandraError, "Failed to bind ASCII parameter at index %zu: String contains non-ASCII characters", param_index);
        } else {
            rb_raise(rb_eCassandraError, "Failed to bind ASCII parameter at index %zu: %s", 
                     param_index, cass_error_desc(error));
        }
    }
    
    return self;
}

// Bind an ASCII value by name (ASCII-only strings)
static VALUE rb_statement_bind_ascii_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_string_to_cass_ascii_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        if (error == CASS_ERROR_LIB_INVALID_VALUE_TYPE) {
            rb_raise(rb_eCassandraError, "Failed to bind ASCII parameter '%s': String contains non-ASCII characters", param_name);
        } else {
            rb_raise(rb_eCassandraError, "Failed to bind ASCII parameter '%s': %s", 
                     param_name, cass_error_desc(error));
        }
    }
    
    return self;
}

// Bind a blob value by index (binary data)
static VALUE rb_statement_bind_blob_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_string_to_cass_blob(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind blob parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a blob value by name (binary data)
static VALUE rb_statement_bind_blob_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_string_to_cass_blob_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind blob parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind an inet value by index (IP addresses)
static VALUE rb_statement_bind_inet_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_inet(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind inet parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind an inet value by name (IP addresses)
static VALUE rb_statement_bind_inet_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_inet_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind inet parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a float value by index (32-bit IEEE 754)
static VALUE rb_statement_bind_float_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_float(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind float parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a float value by name (32-bit IEEE 754)
static VALUE rb_statement_bind_float_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_float_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind float parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a double value by index (64-bit IEEE 754)
static VALUE rb_statement_bind_double_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_double(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind double parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a double value by name (64-bit IEEE 754)
static VALUE rb_statement_bind_double_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_double_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind double parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a decimal value by index (arbitrary precision decimal)
static VALUE rb_statement_bind_decimal_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_decimal(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind decimal parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a decimal value by name (arbitrary precision decimal)
static VALUE rb_statement_bind_decimal_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_decimal_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind decimal parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a UUID value by index
static VALUE rb_statement_bind_uuid_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_uuid(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind UUID parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a UUID value by name
static VALUE rb_statement_bind_uuid_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_uuid_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind UUID parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a TimeUUID value by index
static VALUE rb_statement_bind_timeuuid_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_timeuuid(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind TimeUUID parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a TimeUUID value by name
static VALUE rb_statement_bind_timeuuid_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_timeuuid_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind TimeUUID parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a date value by index
static VALUE rb_statement_bind_date_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_date(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind date parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a date value by name
static VALUE rb_statement_bind_date_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_date_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind date parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a time value by index
static VALUE rb_statement_bind_time_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_time(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind time parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a time value by name
static VALUE rb_statement_bind_time_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_time_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind time parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a timestamp value by index
static VALUE rb_statement_bind_timestamp_by_index(VALUE self, VALUE index, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error = ruby_value_to_cass_timestamp(wrapper->statement, param_index, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind timestamp parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a timestamp value by name
static VALUE rb_statement_bind_timestamp_by_name(VALUE self, VALUE name, VALUE value) {
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error = ruby_value_to_cass_timestamp_by_name(wrapper->statement, param_name, value);
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind timestamp parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Type-hinted collection binding methods

// Bind a list value by index with element type hint
static VALUE rb_statement_bind_list_by_index(int argc, VALUE* argv, VALUE self) {
    VALUE index, value, element_type;
    rb_scan_args(argc, argv, "21", &index, &value, &element_type);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error;
    
    if (NIL_P(element_type)) {
        // Use default list binding without type hint
        error = ruby_value_to_cass_list(wrapper->statement, param_index, value);
    } else {
        // Use type-hinted list binding
        error = ruby_value_to_cass_list_with_type(wrapper->statement, param_index, value, element_type);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind list parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a list value by name with element type hint
static VALUE rb_statement_bind_list_by_name(int argc, VALUE* argv, VALUE self) {
    VALUE name, value, element_type;
    rb_scan_args(argc, argv, "21", &name, &value, &element_type);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error;
    
    if (NIL_P(element_type)) {
        // Use default list binding without type hint
        error = ruby_value_to_cass_list_by_name(wrapper->statement, param_name, value);
    } else {
        // Use type-hinted list binding
        error = ruby_value_to_cass_list_with_type_by_name(wrapper->statement, param_name, value, element_type);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind list parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a set value by index with element type hint
static VALUE rb_statement_bind_set_by_index(int argc, VALUE* argv, VALUE self) {
    VALUE index, value, element_type;
    rb_scan_args(argc, argv, "21", &index, &value, &element_type);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error;
    
    if (NIL_P(element_type)) {
        // Use default set binding without type hint
        error = ruby_value_to_cass_set(wrapper->statement, param_index, value);
    } else {
        // Use type-hinted set binding
        error = ruby_value_to_cass_set_with_type(wrapper->statement, param_index, value, element_type);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind set parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a set value by name with element type hint
static VALUE rb_statement_bind_set_by_name(int argc, VALUE* argv, VALUE self) {
    VALUE name, value, element_type;
    rb_scan_args(argc, argv, "21", &name, &value, &element_type);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error;
    
    if (NIL_P(element_type)) {
        // Use default set binding without type hint
        error = ruby_value_to_cass_set_by_name(wrapper->statement, param_name, value);
    } else {
        // Use type-hinted set binding
        error = ruby_value_to_cass_set_with_type_by_name(wrapper->statement, param_name, value, element_type);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind set parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Bind a map value by index with key and value type hints
static VALUE rb_statement_bind_map_by_index(int argc, VALUE* argv, VALUE self) {
    VALUE index, value, key_type, value_type;
    rb_scan_args(argc, argv, "22", &index, &value, &key_type, &value_type);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    size_t param_index = NUM2SIZET(index);
    CassError error;
    
    if (NIL_P(key_type) && NIL_P(value_type)) {
        // Use default map binding without type hint
        error = ruby_value_to_cass_map(wrapper->statement, param_index, value);
    } else {
        // Use type-hinted map binding
        error = ruby_value_to_cass_map_with_type(wrapper->statement, param_index, value, key_type, value_type);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind map parameter at index %zu: %s", 
                 param_index, cass_error_desc(error));
    }
    
    return self;
}

// Bind a map value by name with key and value type hints
static VALUE rb_statement_bind_map_by_name(int argc, VALUE* argv, VALUE self) {
    VALUE name, value, key_type, value_type;
    rb_scan_args(argc, argv, "22", &name, &value, &key_type, &value_type);
    
    StatementWrapper* wrapper;
    TypedData_Get_Struct(self, StatementWrapper, &statement_type, wrapper);
    
    if (wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }
    
    Check_Type(name, T_STRING);
    const char* param_name = StringValueCStr(name);
    CassError error;
    
    if (NIL_P(key_type) && NIL_P(value_type)) {
        // Use default map binding without type hint
        error = ruby_value_to_cass_map_by_name(wrapper->statement, param_name, value);
    } else {
        // Use type-hinted map binding
        error = ruby_value_to_cass_map_with_type_by_name(wrapper->statement, param_name, value, key_type, value_type);
    }
    
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to bind map parameter '%s': %s", 
                 param_name, cass_error_desc(error));
    }
    
    return self;
}

// Initialize the Statement class within the CassandraC module
VALUE cCassStatement;
void Init_cassandra_c_statement(VALUE module) {
    cCassStatement = rb_define_class_under(module, "Statement", rb_cObject);
    
    rb_define_alloc_func(cCassStatement, rb_statement_allocate);
    rb_define_method(cCassStatement, "initialize", rb_statement_initialize, -1);
    rb_define_method(cCassStatement, "consistency=", rb_statement_set_consistency, 1);
    rb_define_method(cCassStatement, "bind_by_index", rb_statement_bind_by_index, -1);
    rb_define_method(cCassStatement, "bind_by_name", rb_statement_bind_by_name, -1);
    
    // Type-specific binding methods
    rb_define_method(cCassStatement, "bind_text_by_index", rb_statement_bind_text_by_index, 2);
    rb_define_method(cCassStatement, "bind_text_by_name", rb_statement_bind_text_by_name, 2);
    rb_define_method(cCassStatement, "bind_ascii_by_index", rb_statement_bind_ascii_by_index, 2);
    rb_define_method(cCassStatement, "bind_ascii_by_name", rb_statement_bind_ascii_by_name, 2);
    rb_define_method(cCassStatement, "bind_blob_by_index", rb_statement_bind_blob_by_index, 2);
    rb_define_method(cCassStatement, "bind_blob_by_name", rb_statement_bind_blob_by_name, 2);
    rb_define_method(cCassStatement, "bind_inet_by_index", rb_statement_bind_inet_by_index, 2);
    rb_define_method(cCassStatement, "bind_inet_by_name", rb_statement_bind_inet_by_name, 2);
    rb_define_method(cCassStatement, "bind_float_by_index", rb_statement_bind_float_by_index, 2);
    rb_define_method(cCassStatement, "bind_float_by_name", rb_statement_bind_float_by_name, 2);
    rb_define_method(cCassStatement, "bind_double_by_index", rb_statement_bind_double_by_index, 2);
    rb_define_method(cCassStatement, "bind_double_by_name", rb_statement_bind_double_by_name, 2);
    rb_define_method(cCassStatement, "bind_decimal_by_index", rb_statement_bind_decimal_by_index, 2);
    rb_define_method(cCassStatement, "bind_decimal_by_name", rb_statement_bind_decimal_by_name, 2);
    rb_define_method(cCassStatement, "bind_uuid_by_index", rb_statement_bind_uuid_by_index, 2);
    rb_define_method(cCassStatement, "bind_uuid_by_name", rb_statement_bind_uuid_by_name, 2);
    rb_define_method(cCassStatement, "bind_timeuuid_by_index", rb_statement_bind_timeuuid_by_index, 2);
    rb_define_method(cCassStatement, "bind_timeuuid_by_name", rb_statement_bind_timeuuid_by_name, 2);
    rb_define_method(cCassStatement, "bind_date_by_index", rb_statement_bind_date_by_index, 2);
    rb_define_method(cCassStatement, "bind_date_by_name", rb_statement_bind_date_by_name, 2);
    rb_define_method(cCassStatement, "bind_time_by_index", rb_statement_bind_time_by_index, 2);
    rb_define_method(cCassStatement, "bind_time_by_name", rb_statement_bind_time_by_name, 2);
    rb_define_method(cCassStatement, "bind_timestamp_by_index", rb_statement_bind_timestamp_by_index, 2);
    rb_define_method(cCassStatement, "bind_timestamp_by_name", rb_statement_bind_timestamp_by_name, 2);
    
    // Type-hinted collection binding methods
    rb_define_method(cCassStatement, "bind_list_by_index", rb_statement_bind_list_by_index, -1);
    rb_define_method(cCassStatement, "bind_list_by_name", rb_statement_bind_list_by_name, -1);
    rb_define_method(cCassStatement, "bind_set_by_index", rb_statement_bind_set_by_index, -1);
    rb_define_method(cCassStatement, "bind_set_by_name", rb_statement_bind_set_by_name, -1);
    rb_define_method(cCassStatement, "bind_map_by_index", rb_statement_bind_map_by_index, -1);
    rb_define_method(cCassStatement, "bind_map_by_name", rb_statement_bind_map_by_name, -1);
}