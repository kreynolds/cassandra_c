#include "cassandra_c.h"

VALUE rb_cFuture;

// Free function for Future
static void future_free(void* ptr) {
    FutureWrapper* wrapper = (FutureWrapper*)ptr;
    if (wrapper->future != NULL) {
        cass_future_free(wrapper->future);
    }
    xfree(wrapper);
}

// Data type for Future
const rb_data_type_t future_type = {
    .wrap_struct_name = "CassFuture",
    .function = {
        .dmark = NULL,
        .dfree = future_free,
        .dsize = NULL,
    },
    .data = NULL,
    .flags = RUBY_TYPED_FREE_IMMEDIATELY
};

// Allocate function for Future
static VALUE future_allocate(VALUE klass) {
    FutureWrapper* wrapper = ALLOC(FutureWrapper);
    wrapper->future = NULL;
    return TypedData_Wrap_Struct(klass, &future_type, wrapper);
}

VALUE future_new(CassFuture* future) {
    VALUE rb_future = future_allocate(rb_cFuture);
    FutureWrapper* wrapper;
    TypedData_Get_Struct(rb_future, FutureWrapper, &future_type, wrapper);
    wrapper->future = future;
    return rb_future;
}

// Check if the Future is ready
static VALUE future_ready(VALUE self) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);
    return cass_future_ready(wrapper->future) ? Qtrue : Qfalse;
}

// Wait for the Future to complete
static VALUE future_wait(VALUE self) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);
    cass_future_wait(wrapper->future);
    return self;
}

// Wait for the Future to complete with a timeout
static VALUE future_wait_timed(VALUE self, VALUE timeout) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);
    cass_bool_t timed_out = cass_future_wait_timed(wrapper->future, NUM2LL(timeout));
    return timed_out ? Qfalse : Qtrue;
}

// Get the error code from the Future
static VALUE future_error_code(VALUE self) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);
    return INT2NUM(cass_future_error_code(wrapper->future));
}

// Get the error message from the Future
static VALUE future_error_message(VALUE self) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);
    const char* message;
    size_t message_length;
    cass_future_error_message(wrapper->future, &message, &message_length);
    return rb_str_new(message, message_length);
}

// Get the result from the Future
static VALUE future_get_result(VALUE self) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);

    if (cass_future_error_code(wrapper->future) != CASS_OK) {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(wrapper->future, &error_message, &error_message_length);
        rb_raise(rb_eCassandraError, "Future error: %.*s", (int)error_message_length, error_message);
    }

    const CassResult* result = cass_future_get_result(wrapper->future);
    if (result == NULL) {
        return Qnil;
    }

    // Assuming you have a function to create a Ruby Result object
    return Qnil;
    // TODO: This should return a result
    // return rb_result_new(result);
}

// Get the prepared statement from the Future
static VALUE future_get_prepared(VALUE self) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);

    if (cass_future_error_code(wrapper->future) != CASS_OK) {
        const char* error_message;
        size_t error_message_length;
        cass_future_error_message(wrapper->future, &error_message, &error_message_length);
        rb_raise(rb_eCassandraError, "Future error: %.*s", (int)error_message_length, error_message);
    }

    const CassPrepared* prepared = cass_future_get_prepared(wrapper->future);
    if (prepared == NULL) {
        return Qnil;
    }

    // Assuming you have a function to create a Ruby Prepared object
    return prepared_new(prepared);
}

// Get the tracing ID from the Future
static VALUE future_tracing_id(VALUE self) {
    FutureWrapper* wrapper;
    TypedData_Get_Struct(self, FutureWrapper, &future_type, wrapper);

    CassUuid trace_id;
    CassError rc = cass_future_tracing_id(wrapper->future, &trace_id);
    if (rc != CASS_OK) {
        // If there's no tracing ID, return nil
        return Qnil;
    }

    char uuid_str[CASS_UUID_STRING_LENGTH];
    cass_uuid_string(trace_id, uuid_str);

    return rb_str_new_cstr(uuid_str);
}

// Initialize the Future class
void Init_cassandra_c_future(VALUE mCassandraC) {
    rb_cFuture = rb_define_class_under(mCassandraC, "Future", rb_cObject);
    rb_define_alloc_func(rb_cFuture, future_allocate);
    rb_define_method(rb_cFuture, "ready?", future_ready, 0);
    rb_define_method(rb_cFuture, "wait", future_wait, 0);
    rb_define_method(rb_cFuture, "wait_timed", future_wait_timed, 1);
    rb_define_method(rb_cFuture, "error_code", future_error_code, 0);
    rb_define_method(rb_cFuture, "error_message", future_error_message, 0);
    rb_define_method(rb_cFuture, "get_result", future_get_result, 0);
    rb_define_method(rb_cFuture, "get_prepared", future_get_prepared, 0);
    rb_define_method(rb_cFuture, "tracing_id", future_tracing_id, 0);
}