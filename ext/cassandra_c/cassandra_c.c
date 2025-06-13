#include "cassandra_c.h"

/*
 * CassandraC Ruby Extension - Main Module
 *
 * This file contains the main initialization code for the CassandraC Ruby extension.
 * It sets up the module structure, defines constants, and initializes all sub-components.
 */

// ============================================================================
// Module and Exception Definitions
// ============================================================================

VALUE mCassandraC;
VALUE mCassandraCNative;
VALUE rb_eCassandraError;

// ============================================================================
// Error Handling
// ============================================================================

void raise_cassandra_error(CassError error, const char* message) {
    const char* error_desc = cass_error_desc(error);
    rb_raise(rb_eCassandraError, "%s: %s", message, error_desc);
}

void raise_future_error(CassFuture* future, const char* prefix) {
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    
    char* message_copy = malloc(message_length + 1);
    if (message_copy != NULL) {
        memcpy(message_copy, message, message_length);
        message_copy[message_length] = '\0';
        cass_future_free(future);
        rb_raise(rb_eCassandraError, "%s: %s", prefix, message_copy);
        free(message_copy);  // Not reached
    } else {
        cass_future_free(future);
        rb_raise(rb_eCassandraError, "%s: (memory allocation failed for error message)", prefix);
    }
}

// Shared utility function to convert Ruby value to CassConsistency
CassConsistency ruby_value_to_consistency(VALUE consistency) {
    if (TYPE(consistency) == T_FIXNUM) {
        // Allow direct integer values for maximum performance
        return (CassConsistency)NUM2INT(consistency);
    } else if (TYPE(consistency) == T_SYMBOL) {
        // Fast hash lookup for symbols - using the shared global consistency_map
        VALUE value = rb_hash_lookup(consistency_map, consistency);
        if (NIL_P(value)) {
            rb_raise(rb_eArgError, "Invalid consistency level: %s", 
                     RSTRING_PTR(rb_sym2str(consistency)));
        }
        return (CassConsistency)NUM2INT(value);
    } else {
        rb_raise(rb_eArgError, "Consistency must be an integer or symbol");
    }
}

// ============================================================================
// Constants Definition
// ============================================================================

static void define_consistency_constants(VALUE mCassandraC) {
    VALUE mConsistency = rb_define_module_under(mCassandraC, "Consistency");
    
    rb_define_const(mConsistency, "ANY", INT2NUM(CASS_CONSISTENCY_ANY));
    rb_define_const(mConsistency, "ONE", INT2NUM(CASS_CONSISTENCY_ONE));
    rb_define_const(mConsistency, "TWO", INT2NUM(CASS_CONSISTENCY_TWO));
    rb_define_const(mConsistency, "THREE", INT2NUM(CASS_CONSISTENCY_THREE));
    rb_define_const(mConsistency, "QUORUM", INT2NUM(CASS_CONSISTENCY_QUORUM));
    rb_define_const(mConsistency, "ALL", INT2NUM(CASS_CONSISTENCY_ALL));
    rb_define_const(mConsistency, "LOCAL_QUORUM", INT2NUM(CASS_CONSISTENCY_LOCAL_QUORUM));
    rb_define_const(mConsistency, "EACH_QUORUM", INT2NUM(CASS_CONSISTENCY_EACH_QUORUM));
    rb_define_const(mConsistency, "SERIAL", INT2NUM(CASS_CONSISTENCY_SERIAL));
    rb_define_const(mConsistency, "LOCAL_SERIAL", INT2NUM(CASS_CONSISTENCY_LOCAL_SERIAL));
    rb_define_const(mConsistency, "LOCAL_ONE", INT2NUM(CASS_CONSISTENCY_LOCAL_ONE));
}

// ============================================================================
// Main Extension Initialization
// ============================================================================

void Init_cassandra_c(void) {
    // Create the main CassandraC module
    mCassandraC = rb_define_module("CassandraC");

    // Create the Native sub-module for low-level bindings
    mCassandraCNative = rb_define_module_under(mCassandraC, "Native");

    // Define the main exception class
    rb_eCassandraError = rb_define_class_under(mCassandraC, "Error", rb_eRuntimeError);
    
    // Define module constants
    define_consistency_constants(mCassandraC);

    // Initialize all sub-components under the Native module
    Init_cassandra_c_cluster(mCassandraCNative);
    Init_cassandra_c_session(mCassandraCNative);
    Init_cassandra_c_future(mCassandraCNative);
    Init_cassandra_c_result(mCassandraCNative);
    Init_cassandra_c_prepared(mCassandraCNative);
    Init_cassandra_c_statement(mCassandraCNative);
    Init_cassandra_c_batch(mCassandraCNative);
    Init_cassandra_c_timeuuid(mCassandraCNative);
}
