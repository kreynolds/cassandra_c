#include "cassandra_c.h"

VALUE cCassPrepared;

// Free function for Prepared
static void prepared_free(void* ptr) {
    PreparedWrapper* wrapper = (PreparedWrapper*)ptr;
    if (wrapper->prepared != NULL) {
        cass_prepared_free(wrapper->prepared);
    }
    xfree(wrapper);
}

// Data type for Prepared
const rb_data_type_t prepared_type = {
    .wrap_struct_name = "CassPrepared",
    .function = {
        .dmark = NULL,
        .dfree = prepared_free,
        .dsize = NULL,
    },
    .data = NULL,
    .flags = RUBY_TYPED_FREE_IMMEDIATELY
};
// Allocate function for Prepared
static VALUE prepared_allocate(VALUE klass) {
    PreparedWrapper* wrapper = ALLOC(PreparedWrapper);
    wrapper->prepared = NULL;
    return TypedData_Wrap_Struct(klass, &prepared_type, wrapper);
}

VALUE prepared_new(const CassPrepared* prepared) {
    VALUE rb_prepared = prepared_allocate(cCassPrepared);
    PreparedWrapper* wrapper;
    TypedData_Get_Struct(rb_prepared, PreparedWrapper, &prepared_type, wrapper);
    wrapper->prepared = prepared;
    return rb_prepared;
}

// Bind method - creates a statement from this prepared statement
static VALUE prepared_bind(VALUE self) {
    PreparedWrapper* wrapper;
    TypedData_Get_Struct(self, PreparedWrapper, &prepared_type, wrapper);
    
    if (wrapper->prepared == NULL) {
        rb_raise(rb_eCassandraError, "Prepared statement is NULL");
    }
    
    CassStatement* statement = cass_prepared_bind(wrapper->prepared);
    if (statement == NULL) {
        rb_raise(rb_eCassandraError, "Failed to bind prepared statement");
    }
    
    return statement_new(statement);
}

void Init_cassandra_c_prepared(VALUE mCassandraC) {
    cCassPrepared = rb_define_class_under(mCassandraC, "Prepared", rb_cObject);
    rb_define_alloc_func(cCassPrepared, prepared_allocate);
    rb_define_method(cCassPrepared, "bind", prepared_bind, 0);
}
