#include "cassandra_c.h"

// Memory management for Session
static void rb_session_free(void* ptr) {
    SessionWrapper* wrapper = (SessionWrapper*)ptr;
    if (wrapper->session != NULL) {
        cass_session_free(wrapper->session);
    }
    xfree(wrapper);
}

// Define the Ruby data type for Session
const rb_data_type_t session_type = {
    .wrap_struct_name = "CassSession",
    .function = {
        .dmark = NULL,
        .dfree = rb_session_free,
        .dsize = NULL,
    },
    .data = NULL,
    .flags = RUBY_TYPED_FREE_IMMEDIATELY
};

// Allocation function for Session
static VALUE rb_session_allocate(VALUE klass) {
    SessionWrapper* wrapper = ALLOC(SessionWrapper);
    wrapper->session = cass_session_new();
    if (!wrapper->session) {
        rb_raise(rb_eRuntimeError, "Failed to create CassSession");
    }
    return TypedData_Wrap_Struct(klass, &session_type, wrapper);
}

// Initialize method for Session (no arguments)
static VALUE rb_session_initialize(VALUE self) {
    // No initialization required beyond allocation
    return self;
}

// Connect method for Session
static VALUE rb_session_connect(int argc, VALUE* argv, VALUE self) {
    VALUE cluster, options;
    rb_scan_args(argc, argv, "1:", &cluster, &options);

    SessionWrapper* wrapper;
    TypedData_Get_Struct(self, SessionWrapper, &session_type, wrapper);

    // Retrieve the ClusterWrapper from the Ruby Cluster object
    ClusterWrapper* cluster_wrapper;
    TypedData_Get_Struct(cluster, ClusterWrapper, &cluster_type, cluster_wrapper);

    // Attempt to connect the session to the cluster
    CassFuture* connect_future = cass_session_connect(wrapper->session, cluster_wrapper->cluster);

    // Check if async option is provided and true
    VALUE async = Qfalse;
    if (!NIL_P(options)) {
        async = rb_hash_aref(options, ID2SYM(rb_intern("async")));
    }

    if (RTEST(async)) {
        // Return a Future object for async operation
        return future_new(connect_future);
    } else {
        // Wait for the connection to complete
        cass_future_wait(connect_future);

        // Check for errors during connection
        CassError error = cass_future_error_code(connect_future);
        if (error != CASS_OK) {
            raise_future_error(connect_future, "Failed to connect to Cassandra");
        }

        cass_future_free(connect_future);
        return self;
    }
}

// Close method for Session
static VALUE rb_session_close(VALUE self) {
    SessionWrapper* wrapper;
    TypedData_Get_Struct(self, SessionWrapper, &session_type, wrapper);

    if (wrapper->session != NULL) {
        CassFuture* close_future = cass_session_close(wrapper->session);
        cass_future_wait(close_future);

        CassError error = cass_future_error_code(close_future);
        if (error != CASS_OK) {
            raise_future_error(close_future, "Failed to close Cassandra session");
        }

        cass_future_free(close_future);
    }

    return Qnil;
}

// Prepare method for Session
static VALUE rb_session_prepare(int argc, VALUE* argv, VALUE self) {
    VALUE query, options;
    rb_scan_args(argc, argv, "1:", &query, &options);

    SessionWrapper* wrapper;
    TypedData_Get_Struct(self, SessionWrapper, &session_type, wrapper);

    Check_Type(query, T_STRING);

    CassFuture* prepare_future = cass_session_prepare(wrapper->session, StringValueCStr(query));

    // Check if async option is provided and true
    VALUE async = Qfalse;
    if (!NIL_P(options)) {
        async = rb_hash_aref(options, ID2SYM(rb_intern("async")));
    }

    if (RTEST(async)) {
        // Return a Future object for async operation
        return future_new(prepare_future);
    } else {
        // Wait for the preparation to complete
        cass_future_wait(prepare_future);

        // Check for errors during preparation
        CassError error = cass_future_error_code(prepare_future);
        if (error != CASS_OK) {
            raise_future_error(prepare_future, "Failed to prepare statement");
        }

        const CassPrepared* prepared = cass_future_get_prepared(prepare_future);

        VALUE rb_prepared = prepared_new(prepared);

        cass_future_free(prepare_future);

        return rb_prepared;
      // return Qnil;
    }
}

// Get the client_id
static VALUE rb_session_get_client_id(VALUE self) {
    SessionWrapper* wrapper;
    TypedData_Get_Struct(self, SessionWrapper, &session_type, wrapper);

    CassUuid client_id = cass_session_get_client_id(wrapper->session);

    char uuid_str[CASS_UUID_STRING_LENGTH];
    cass_uuid_string(client_id, uuid_str);

    return rb_str_new_cstr(uuid_str);
}

// Execute a statement
static VALUE rb_session_execute(int argc, VALUE* argv, VALUE self) {
    VALUE statement, options;
    rb_scan_args(argc, argv, "1:", &statement, &options);

    SessionWrapper* wrapper;
    TypedData_Get_Struct(self, SessionWrapper, &session_type, wrapper);

    // Declare future variable
    CassFuture* future;
    
    // Extract the CassStatement from the Ruby Statement object
    StatementWrapper* statement_wrapper;

    if (rb_obj_is_kind_of(statement, cCassStatement)) {
        TypedData_Get_Struct(statement, StatementWrapper, &statement_type, statement_wrapper);
    } else if (TYPE(statement) == T_STRING) {
        // If a string is provided, create a temporary Statement object
        CassStatement* cass_statement = cass_statement_new(StringValueCStr(statement), 0);
        if (!cass_statement) {
            rb_raise(rb_eCassandraError, "Failed to create statement from query string");
        }
        
        // Execute the query and capture the future
        future = cass_session_execute(wrapper->session, cass_statement);
        
        // Free the temporary statement since it's no longer needed
        cass_statement_free(cass_statement);
        
        // Process the future (code continues below)
        statement_wrapper = NULL; // Not used in this path
    } else {
        rb_raise(rb_eTypeError, "Expected Statement object or query string");
        return Qnil;  // Not reached
    }

    // Execute the query and capture the future
    if (statement_wrapper) {
        future = cass_session_execute(wrapper->session, statement_wrapper->statement);
    }
    // future already set if string path was taken
    
    // Check if async option is provided and true
    VALUE async = Qfalse;
    if (!NIL_P(options)) {
        async = rb_hash_aref(options, ID2SYM(rb_intern("async")));
    }

    if (RTEST(async)) {
        // Return a Future object for async operation
        return future_new(future);
    } else {
        // Wait for the execution to complete
        cass_future_wait(future);

        // Check for errors
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            raise_future_error(future, "Failed to execute statement");
        }

        // Get the result
        const CassResult* result = cass_future_get_result(future);
        // Cast away const since we transfer ownership to Ruby's GC via result_new
        VALUE rb_result = result_new((CassResult*)result);
        
        cass_future_free(future);
        
        return rb_result;
    }
}

// Execute a batch statement
static VALUE rb_session_execute_batch(int argc, VALUE* argv, VALUE self) {
    VALUE batch, options;
    rb_scan_args(argc, argv, "1:", &batch, &options);

    SessionWrapper* wrapper;
    TypedData_Get_Struct(self, SessionWrapper, &session_type, wrapper);

    BatchWrapper* batch_wrapper;
    TypedData_Get_Struct(batch, BatchWrapper, &batch_type, batch_wrapper);

    if (batch_wrapper->batch == NULL) {
        rb_raise(rb_eCassandraError, "Batch is NULL");
    }

    // Execute the batch and capture the future
    CassFuture* future = cass_session_execute_batch(wrapper->session, batch_wrapper->batch);

    // Check if async option is provided and true
    VALUE async = Qfalse;
    if (!NIL_P(options)) {
        async = rb_hash_aref(options, ID2SYM(rb_intern("async")));
    }

    if (RTEST(async)) {
        // Return a Future object for async operation
        return future_new(future);
    } else {
        // Wait for the execution to complete
        cass_future_wait(future);

        // Check for errors
        CassError error = cass_future_error_code(future);
        if (error != CASS_OK) {
            raise_future_error(future, "Failed to execute batch");
        }

        // Get the result
        const CassResult* result = cass_future_get_result(future);
        // Cast away const since we transfer ownership to Ruby's GC via result_new
        VALUE rb_result = result_new((CassResult*)result);
        
        cass_future_free(future);
        
        return rb_result;
    }
}

// Execute a query - convenience method that creates a statement and executes it
static VALUE rb_session_query(int argc, VALUE* argv, VALUE self) {
    return rb_session_execute(argc, argv, self);
}

// Initialize the Session class within the CassandraC module
void Init_cassandra_c_session(VALUE module) {
    VALUE cSession = rb_define_class_under(module, "Session", rb_cObject);
 
    rb_define_alloc_func(cSession, rb_session_allocate);
    rb_define_method(cSession, "initialize", rb_session_initialize, 0);
    rb_define_method(cSession, "connect", rb_session_connect, -1);
    rb_define_method(cSession, "close", rb_session_close, 0);
    rb_define_method(cSession, "client_id", rb_session_get_client_id, 0);
    rb_define_method(cSession, "prepare", rb_session_prepare, -1);
    rb_define_method(cSession, "execute", rb_session_execute, -1);
    rb_define_method(cSession, "execute_batch", rb_session_execute_batch, -1);
    rb_define_method(cSession, "query", rb_session_query, -1);
}
 