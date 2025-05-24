#include "cassandra_c.h"

VALUE cCassResult;

// Memory management for Result
static void result_free(void* ptr) {
    ResultWrapper* wrapper = (ResultWrapper*)ptr;
    if (wrapper->result != NULL) {
        cass_result_free(wrapper->result);
    }
    xfree(wrapper);
}

// Data type for Future
const rb_data_type_t result_type = {
    .wrap_struct_name = "CassResult",
    .function = {
        .dmark = NULL,
        .dfree = result_free,
        .dsize = NULL,
    },
    .data = NULL,
    .flags = RUBY_TYPED_FREE_IMMEDIATELY
};

// Allocate function for Result
static VALUE result_allocate(VALUE klass) {
    ResultWrapper* wrapper = ALLOC(ResultWrapper);
    wrapper->result = NULL;
    return TypedData_Wrap_Struct(klass, &result_type, wrapper);
}

// Initialize method for Result
static VALUE result_initialize(VALUE self, VALUE wrapped_result) {
    ResultWrapper* wrapper;
    TypedData_Get_Struct(self, ResultWrapper, &result_type, wrapper);
    
    if (!rb_obj_is_kind_of(wrapped_result, rb_cObject)) {
        rb_raise(rb_eTypeError, "wrapped_result must be a CassResult pointer");
    }
    
    wrapper->result = (CassResult*)NUM2ULL(wrapped_result);
    return self;
}

// Create a new Result object wrapping a CassResult
VALUE result_new(CassResult* result) {
    VALUE rb_result = result_allocate(cCassResult);
    return result_initialize(rb_result, ULL2NUM((unsigned long long)result));
}

// Get the row count
static VALUE result_row_count(VALUE self) {
    ResultWrapper* wrapper;
    TypedData_Get_Struct(self, ResultWrapper, &result_type, wrapper);
    
    size_t count = cass_result_row_count(wrapper->result);
    return ULONG2NUM(count);
}

// Get the column count
static VALUE result_column_count(VALUE self) {
    ResultWrapper* wrapper;
    TypedData_Get_Struct(self, ResultWrapper, &result_type, wrapper);
    
    size_t count = cass_result_column_count(wrapper->result);
    return ULONG2NUM(count);
}

// Check if the result has more pages
static VALUE result_has_more_pages(VALUE self) {
    ResultWrapper* wrapper;
    TypedData_Get_Struct(self, ResultWrapper, &result_type, wrapper);
    
    cass_bool_t has_more = cass_result_has_more_pages(wrapper->result);
    return has_more ? Qtrue : Qfalse;
}

// Get column names
static VALUE result_column_names(VALUE self) {
    ResultWrapper* wrapper;
    TypedData_Get_Struct(self, ResultWrapper, &result_type, wrapper);
    
    // Check for cached value in instance variable
    VALUE column_names = rb_iv_get(self, "@column_names");
    if (NIL_P(column_names)) {
        size_t column_count = cass_result_column_count(wrapper->result);
        column_names = rb_ary_new_capa(column_count);
        
        for (size_t i = 0; i < column_count; i++) {
            const char* column_name;
            size_t column_name_length;
            cass_result_column_name(wrapper->result, i, &column_name, &column_name_length);
            rb_ary_push(column_names, rb_str_new(column_name, column_name_length));
        }
        
        // Cache the column names
        rb_iv_set(self, "@column_names", column_names);
    }
    
    return column_names;
}

// Implement the each method for Enumerable support
static VALUE result_each(VALUE self) {
    RETURN_ENUMERATOR(self, 0, 0);  // Return enumerator if no block given
    
    ResultWrapper* wrapper;
    TypedData_Get_Struct(self, ResultWrapper, &result_type, wrapper);
    
    size_t column_count = cass_result_column_count(wrapper->result);
    
    // Iterate through each row
    CassIterator* rows_iterator = cass_iterator_from_result(wrapper->result);
    while (cass_iterator_next(rows_iterator)) {
        const CassRow* row = cass_iterator_get_row(rows_iterator);
        
        // Create an array for this row's values
        VALUE row_array = rb_ary_new_capa(column_count);
        
        // Extract each column value
        for (size_t i = 0; i < column_count; i++) {
            const CassValue* value = cass_row_get_column(row, i);
            rb_ary_push(row_array, cass_value_to_ruby(value));
        }
        
        // Yield the array of values to the block
        rb_yield(row_array);
    }
    
    cass_iterator_free(rows_iterator);
    
    return self;
}

// Initialize the Result class
void Init_cassandra_c_result(VALUE module) {
    cCassResult = rb_define_class_under(module, "Result", rb_cObject);
    rb_define_alloc_func(cCassResult, result_allocate);
    rb_define_method(cCassResult, "initialize", result_initialize, 1);
    rb_define_method(cCassResult, "row_count", result_row_count, 0);
    rb_define_method(cCassResult, "column_count", result_column_count, 0);
    rb_define_method(cCassResult, "has_more_pages?", result_has_more_pages, 0);
    rb_define_method(cCassResult, "column_names", result_column_names, 0);
    rb_define_method(cCassResult, "each", result_each, 0);
    
    // Include Enumerable to get all the Enumerable methods
    rb_include_module(cCassResult, rb_mEnumerable);
}