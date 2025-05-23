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
VALUE rb_eCassandraError;

// ============================================================================
// Error Handling
// ============================================================================

void raise_cassandra_error(CassError error, const char* message) {
    const char* error_desc = cass_error_desc(error);
    rb_raise(rb_eCassandraError, "%s: %s", message, error_desc);
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

    // Define the main exception class
    rb_eCassandraError = rb_define_class_under(mCassandraC, "Error", rb_eRuntimeError);
    
    // Define module constants
    define_consistency_constants(mCassandraC);

    // Initialize all sub-components
    Init_cassandra_c_cluster(mCassandraC);
    Init_cassandra_c_session(mCassandraC);
    Init_cassandra_c_future(mCassandraC);
    Init_cassandra_c_result(mCassandraC);
    Init_cassandra_c_prepared(mCassandraC);
    Init_cassandra_c_statement(mCassandraC);
}
