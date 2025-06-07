#include "cassandra_c.h"

// Memory management for Batch
static void rb_batch_free(void* ptr) {
    BatchWrapper* wrapper = (BatchWrapper*)ptr;
    if (wrapper->batch != NULL) {
        cass_batch_free(wrapper->batch);
    }
    xfree(wrapper);
}

// Define the Ruby data type for Batch
const rb_data_type_t batch_type = {
    .wrap_struct_name = "CassBatch",
    .function = {
        .dmark = NULL,
        .dfree = rb_batch_free,
        .dsize = NULL,
    },
    .data = NULL,
    .flags = RUBY_TYPED_FREE_IMMEDIATELY
};

// Create a new Batch object
VALUE batch_new(CassBatch* batch) {
    BatchWrapper* wrapper = ALLOC(BatchWrapper);
    wrapper->batch = batch;
    VALUE rb_batch = TypedData_Wrap_Struct(cCassBatch, &batch_type, wrapper);
    return rb_batch;
}

// Allocation function for Batch
static VALUE rb_batch_allocate(VALUE klass) {
    BatchWrapper* wrapper = ALLOC(BatchWrapper);
    wrapper->batch = NULL; // Will be set in initialize
    return TypedData_Wrap_Struct(klass, &batch_type, wrapper);
}

// Initialize method for Batch
static VALUE rb_batch_initialize(int argc, VALUE* argv, VALUE self) {
    VALUE batch_type_value;
    rb_scan_args(argc, argv, "01", &batch_type_value);

    BatchWrapper* wrapper;
    TypedData_Get_Struct(self, BatchWrapper, &batch_type, wrapper);

    // Free any existing batch
    if (wrapper->batch != NULL) {
        cass_batch_free(wrapper->batch);
    }

    // Determine batch type
    CassBatchType type = CASS_BATCH_TYPE_LOGGED; // Default
    if (!NIL_P(batch_type_value)) {
        if (TYPE(batch_type_value) == T_SYMBOL) {
            VALUE sym_str = rb_sym2str(batch_type_value);
            const char* type_str = StringValueCStr(sym_str);
            
            if (strcmp(type_str, "logged") == 0) {
                type = CASS_BATCH_TYPE_LOGGED;
            } else if (strcmp(type_str, "unlogged") == 0) {
                type = CASS_BATCH_TYPE_UNLOGGED;
            } else if (strcmp(type_str, "counter") == 0) {
                type = CASS_BATCH_TYPE_COUNTER;
            } else {
                rb_raise(rb_eArgError, "Invalid batch type: %s (valid: :logged, :unlogged, :counter)", type_str);
            }
        } else if (TYPE(batch_type_value) == T_FIXNUM) {
            type = (CassBatchType)NUM2INT(batch_type_value);
        } else {
            rb_raise(rb_eArgError, "Batch type must be a symbol or integer");
        }
    }

    wrapper->batch = cass_batch_new(type);

    if (!wrapper->batch) {
        rb_raise(rb_eCassandraError, "Failed to create batch");
    }

    return self;
}

// Set consistency for this batch
static VALUE rb_batch_set_consistency(VALUE self, VALUE consistency) {
    BatchWrapper* wrapper;
    TypedData_Get_Struct(self, BatchWrapper, &batch_type, wrapper);

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

    CassError error = cass_batch_set_consistency(wrapper->batch, consistency_value);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to set batch consistency level: %s", cass_error_desc(error));
    }

    return self;
}

// Set serial consistency for this batch
static VALUE rb_batch_set_serial_consistency(VALUE self, VALUE serial_consistency) {
    BatchWrapper* wrapper;
    TypedData_Get_Struct(self, BatchWrapper, &batch_type, wrapper);

    CassConsistency consistency_value;

    if (TYPE(serial_consistency) == T_FIXNUM) {
        consistency_value = (CassConsistency)NUM2INT(serial_consistency);
    } else if (TYPE(serial_consistency) == T_SYMBOL) {
        VALUE value = rb_hash_lookup(consistency_map, serial_consistency);
        if (NIL_P(value)) {
            rb_raise(rb_eArgError, "Invalid serial consistency level: %s", 
                     RSTRING_PTR(rb_sym2str(serial_consistency)));
        }
        consistency_value = (CassConsistency)NUM2INT(value);
    } else {
        rb_raise(rb_eArgError, "Serial consistency must be an integer or symbol");
    }

    CassError error = cass_batch_set_serial_consistency(wrapper->batch, consistency_value);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to set batch serial consistency level: %s", cass_error_desc(error));
    }

    return self;
}

// Set timestamp for this batch
static VALUE rb_batch_set_timestamp(VALUE self, VALUE timestamp) {
    BatchWrapper* wrapper;
    TypedData_Get_Struct(self, BatchWrapper, &batch_type, wrapper);

    cass_int64_t timestamp_value = NUM2LL(timestamp);

    CassError error = cass_batch_set_timestamp(wrapper->batch, timestamp_value);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to set batch timestamp: %s", cass_error_desc(error));
    }

    return self;
}

// Set request timeout for this batch
static VALUE rb_batch_set_request_timeout(VALUE self, VALUE timeout_ms) {
    BatchWrapper* wrapper;
    TypedData_Get_Struct(self, BatchWrapper, &batch_type, wrapper);

    cass_uint64_t timeout_value = NUM2ULL(timeout_ms);

    CassError error = cass_batch_set_request_timeout(wrapper->batch, timeout_value);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to set batch request timeout: %s", cass_error_desc(error));
    }

    return self;
}

// Set idempotent flag for this batch
static VALUE rb_batch_set_is_idempotent(VALUE self, VALUE is_idempotent) {
    BatchWrapper* wrapper;
    TypedData_Get_Struct(self, BatchWrapper, &batch_type, wrapper);

    cass_bool_t idempotent_value = RTEST(is_idempotent) ? cass_true : cass_false;

    CassError error = cass_batch_set_is_idempotent(wrapper->batch, idempotent_value);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to set batch idempotent flag: %s", cass_error_desc(error));
    }

    return self;
}

// Add a statement to the batch
static VALUE rb_batch_add_statement(VALUE self, VALUE statement) {
    BatchWrapper* wrapper;
    TypedData_Get_Struct(self, BatchWrapper, &batch_type, wrapper);

    if (wrapper->batch == NULL) {
        rb_raise(rb_eCassandraError, "Batch is NULL");
    }

    StatementWrapper* statement_wrapper;
    TypedData_Get_Struct(statement, StatementWrapper, &statement_type, statement_wrapper);

    if (statement_wrapper->statement == NULL) {
        rb_raise(rb_eCassandraError, "Statement is NULL");
    }

    CassError error = cass_batch_add_statement(wrapper->batch, statement_wrapper->statement);
    if (error != CASS_OK) {
        rb_raise(rb_eCassandraError, "Failed to add statement to batch: %s", cass_error_desc(error));
    }

    return self;
}

// Initialize the Batch class within the CassandraC module
VALUE cCassBatch;
void Init_cassandra_c_batch(VALUE module) {
    cCassBatch = rb_define_class_under(module, "Batch", rb_cObject);

    rb_define_alloc_func(cCassBatch, rb_batch_allocate);
    rb_define_method(cCassBatch, "initialize", rb_batch_initialize, -1);
    rb_define_method(cCassBatch, "consistency=", rb_batch_set_consistency, 1);
    rb_define_method(cCassBatch, "serial_consistency=", rb_batch_set_serial_consistency, 1);
    rb_define_method(cCassBatch, "timestamp=", rb_batch_set_timestamp, 1);
    rb_define_method(cCassBatch, "request_timeout=", rb_batch_set_request_timeout, 1);
    rb_define_method(cCassBatch, "idempotent=", rb_batch_set_is_idempotent, 1);
    rb_define_method(cCassBatch, "add", rb_batch_add_statement, 1);

    // Define constants for batch types
    rb_define_const(cCassBatch, "LOGGED", INT2NUM(CASS_BATCH_TYPE_LOGGED));
    rb_define_const(cCassBatch, "UNLOGGED", INT2NUM(CASS_BATCH_TYPE_UNLOGGED));
    rb_define_const(cCassBatch, "COUNTER", INT2NUM(CASS_BATCH_TYPE_COUNTER));
}