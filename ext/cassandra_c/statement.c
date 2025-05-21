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
    
    CassConsistency consistency_value;
    
    if (TYPE(consistency) == T_FIXNUM) {
        // Allow direct integer values for maximum performance
        consistency_value = (CassConsistency)NUM2INT(consistency);
    } else if (TYPE(consistency) == T_SYMBOL) {
        // Fast hash lookup for symbols - using the shared global consistency_map
        VALUE value = rb_hash_lookup(consistency_map, consistency);
        if (NIL_P(value)) {
            rb_raise(rb_eArgError, "Invalid consistency level: %s", 
                     RSTRING_PTR(rb_sym2str(consistency)));
        }
        consistency_value = (CassConsistency)NUM2INT(value);
    } else {
        rb_raise(rb_eArgError, "Consistency must be an integer or symbol");
    }
    
    CassError error = cass_statement_set_consistency(wrapper->statement, consistency_value);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to set consistency level: %s", cass_error_desc(error));
    }
    
    return self;
}

// Initialize the Statement class within the CassandraC module
VALUE cCassStatement;
void Init_cassandra_c_statement(VALUE mCassandraC) {
    cCassStatement = rb_define_class_under(mCassandraC, "Statement", rb_cObject);
    
    rb_define_alloc_func(cCassStatement, rb_statement_allocate);
    rb_define_method(cCassStatement, "initialize", rb_statement_initialize, -1);
    rb_define_method(cCassStatement, "consistency=", rb_statement_set_consistency, 1);
}