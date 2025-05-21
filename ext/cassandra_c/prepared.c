#include "cassandra_c.h"

VALUE rb_cPrepared;

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
    .parent = NULL,
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
    VALUE rb_prepared = prepared_allocate(rb_cPrepared);
    PreparedWrapper* wrapper;
    TypedData_Get_Struct(rb_prepared, PreparedWrapper, &prepared_type, wrapper);
    wrapper->prepared = prepared;
    return rb_prepared;
}

void Init_cassandra_c_prepared(VALUE mCassandraC) {
    rb_cPrepared = rb_define_class_under(mCassandraC, "Prepared", rb_cObject);
    rb_define_alloc_func(rb_cPrepared, prepared_allocate);
}
