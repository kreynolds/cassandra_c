#include "cassandra_c.h"
#include <time.h>

// TimeUuid wrapper structure
typedef struct {
    CassUuid uuid;
    CassUuidGen* uuid_gen;
} TimeUuidWrapper;

// Forward declarations
static VALUE rb_timeuuid_allocate(VALUE klass);
static void rb_timeuuid_free(void* data);
static size_t rb_timeuuid_memsize(const void* data);

// Type definition for Ruby's garbage collector
static const rb_data_type_t timeuuid_type = {
    "CassandraC::Native::TimeUuid",
    {
        NULL,  // mark function (not needed)
        rb_timeuuid_free,
        rb_timeuuid_memsize,
        NULL,  // compact function (not needed)
    },
    NULL, NULL,
    RUBY_TYPED_FREE_IMMEDIATELY
};

// Global UUID generator (thread-safe according to C driver docs)
static CassUuidGen* global_uuid_gen = NULL;

// Initialize global UUID generator
static void init_uuid_generator() {
    if (global_uuid_gen == NULL) {
        global_uuid_gen = cass_uuid_gen_new();
    }
}

// Memory management functions
static VALUE rb_timeuuid_allocate(VALUE klass) {
    TimeUuidWrapper* wrapper = ALLOC(TimeUuidWrapper);
    wrapper->uuid_gen = NULL;  // Will use global generator
    return TypedData_Wrap_Struct(klass, &timeuuid_type, wrapper);
}

static void rb_timeuuid_free(void* data) {
    if (data) {
        TimeUuidWrapper* wrapper = (TimeUuidWrapper*)data;
        // Don't free the global UUID generator
        xfree(wrapper);
    }
}

static size_t rb_timeuuid_memsize(const void* data) {
    return sizeof(TimeUuidWrapper);
}

// Initialize with optional time parameter
static VALUE rb_timeuuid_initialize(int argc, VALUE* argv, VALUE self) {
    VALUE time_value;
    rb_scan_args(argc, argv, "01", &time_value);
    
    TimeUuidWrapper* wrapper;
    TypedData_Get_Struct(self, TimeUuidWrapper, &timeuuid_type, wrapper);
    
    init_uuid_generator();
    
    if (NIL_P(time_value)) {
        // Generate TimeUUID for current time
        cass_uuid_gen_time(global_uuid_gen, &wrapper->uuid);
    } else if (TYPE(time_value) == T_STRING) {
        // Parse UUID string and validate it's a TimeUUID (version 1)
        const char* uuid_str = StringValueCStr(time_value);
        CassError error = cass_uuid_from_string(uuid_str, &wrapper->uuid);
        if (error != CASS_OK) {
            rb_raise(rb_eArgError, "Invalid UUID string format");
        }
        
        // Check if it's a TimeUUID (version 1) - version is in the 13th character (index 12)
        if (strlen(uuid_str) != 36 || uuid_str[14] != '1') {
            rb_raise(rb_eArgError, "UUID must be version 1 (TimeUUID), got version %c", uuid_str[14]);
        }
    } else if (TYPE(time_value) == T_DATA) {
        // Generate TimeUUID for specific time (Ruby Time objects are T_DATA)
        VALUE time_class = rb_const_get(rb_cObject, rb_intern("Time"));
        if (!rb_obj_is_kind_of(time_value, time_class)) {
            rb_raise(rb_eArgError, "Expected Time object, String, or nil");
        }
        
        // Extract timestamp from Ruby Time object
        VALUE time_float = rb_funcall(time_value, rb_intern("to_f"), 0);
        double time_seconds = NUM2DBL(time_float);
        cass_uint64_t timestamp_ms = (cass_uint64_t)(time_seconds * 1000.0);
        
        cass_uuid_gen_from_time(global_uuid_gen, timestamp_ms, &wrapper->uuid);
    } else {
        rb_raise(rb_eArgError, "Expected Time object, String, or nil, got %s", rb_obj_classname(time_value));
    }
    
    return self;
}

// Generate TimeUUID for current time (class method)
static VALUE rb_timeuuid_generate(VALUE klass) {
    VALUE instance = rb_timeuuid_allocate(klass);
    rb_timeuuid_initialize(0, NULL, instance);
    return instance;
}

// Generate TimeUUID from specific time (class method)
static VALUE rb_timeuuid_from_time(VALUE klass, VALUE time_value) {
    VALUE instance = rb_timeuuid_allocate(klass);
    VALUE args[] = { time_value };
    rb_timeuuid_initialize(1, args, instance);
    return instance;
}

// Convert TimeUUID to string representation
static VALUE rb_timeuuid_to_s(VALUE self) {
    TimeUuidWrapper* wrapper;
    TypedData_Get_Struct(self, TimeUuidWrapper, &timeuuid_type, wrapper);
    
    char uuid_str[CASS_UUID_STRING_LENGTH];
    cass_uuid_string(wrapper->uuid, uuid_str);
    
    return rb_str_new_cstr(uuid_str);
}

// Extract timestamp from TimeUUID
static VALUE rb_timeuuid_timestamp(VALUE self) {
    TimeUuidWrapper* wrapper;
    TypedData_Get_Struct(self, TimeUuidWrapper, &timeuuid_type, wrapper);
    
    cass_uint64_t timestamp_ms = cass_uuid_timestamp(wrapper->uuid);
    double timestamp_seconds = (double)timestamp_ms / 1000.0;
    
    // Convert to Ruby Time object
    VALUE time_class = rb_const_get(rb_cObject, rb_intern("Time"));
    return rb_funcall(time_class, rb_intern("at"), 1, rb_float_new(timestamp_seconds));
}

// Alias for timestamp
static VALUE rb_timeuuid_to_time(VALUE self) {
    return rb_timeuuid_timestamp(self);
}

// Equality comparison
static VALUE rb_timeuuid_equal(VALUE self, VALUE other) {
    TimeUuidWrapper* wrapper1;
    TypedData_Get_Struct(self, TimeUuidWrapper, &timeuuid_type, wrapper1);
    
    if (rb_obj_is_kind_of(other, rb_obj_class(self))) {
        // Compare with another TimeUuid object
        TimeUuidWrapper* wrapper2;
        TypedData_Get_Struct(other, TimeUuidWrapper, &timeuuid_type, wrapper2);
        return memcmp(&wrapper1->uuid, &wrapper2->uuid, sizeof(CassUuid)) == 0 ? Qtrue : Qfalse;
    } else if (TYPE(other) == T_STRING) {
        // Compare with string representation
        VALUE self_str = rb_timeuuid_to_s(self);
        return rb_str_equal(self_str, other);
    }
    
    return Qfalse;
}

// Hash value for use in Hash tables
static VALUE rb_timeuuid_hash(VALUE self) {
    TimeUuidWrapper* wrapper;
    TypedData_Get_Struct(self, TimeUuidWrapper, &timeuuid_type, wrapper);
    
    // Use first 8 bytes of UUID as hash (simple but effective)
    cass_uint64_t hash_val = 0;
    memcpy(&hash_val, &wrapper->uuid, sizeof(cass_uint64_t));
    
    return ULL2NUM(hash_val);
}

// Spaceship operator for comparison
static VALUE rb_timeuuid_compare(VALUE self, VALUE other) {
    if (!rb_obj_is_kind_of(other, rb_obj_class(self))) {
        return Qnil;
    }
    
    TimeUuidWrapper* wrapper1;
    TimeUuidWrapper* wrapper2;
    TypedData_Get_Struct(self, TimeUuidWrapper, &timeuuid_type, wrapper1);
    TypedData_Get_Struct(other, TimeUuidWrapper, &timeuuid_type, wrapper2);
    
    // Compare timestamps first (TimeUUIDs are time-ordered)
    cass_uint64_t ts1 = cass_uuid_timestamp(wrapper1->uuid);
    cass_uint64_t ts2 = cass_uuid_timestamp(wrapper2->uuid);
    
    if (ts1 < ts2) return INT2NUM(-1);
    if (ts1 > ts2) return INT2NUM(1);
    
    // If timestamps are equal, compare full UUID
    int result = memcmp(&wrapper1->uuid, &wrapper2->uuid, sizeof(CassUuid));
    if (result < 0) return INT2NUM(-1);
    if (result > 0) return INT2NUM(1);
    return INT2NUM(0);
}

// Type checker method
static VALUE rb_timeuuid_is_timeuuid(VALUE self) {
    return Qtrue;
}

// Get the internal CassUuid (for use by other C extension functions)
CassUuid rb_timeuuid_get_cass_uuid(VALUE timeuuid_obj) {
    TimeUuidWrapper* wrapper;
    TypedData_Get_Struct(timeuuid_obj, TimeUuidWrapper, &timeuuid_type, wrapper);
    return wrapper->uuid;
}

// Create Ruby TimeUuid object from CassUuid (for result parsing)
VALUE rb_timeuuid_from_cass_uuid(CassUuid uuid) {
    VALUE klass = rb_const_get(mCassandraCNative, rb_intern("TimeUuid"));
    VALUE instance = rb_timeuuid_allocate(klass);
    
    TimeUuidWrapper* wrapper;
    TypedData_Get_Struct(instance, TimeUuidWrapper, &timeuuid_type, wrapper);
    wrapper->uuid = uuid;
    
    return instance;
}

// Initialize the TimeUuid class
VALUE cCassTimeUuid;
void Init_cassandra_c_timeuuid(VALUE module) {
    cCassTimeUuid = rb_define_class_under(module, "TimeUuid", rb_cObject);
    
    rb_define_alloc_func(cCassTimeUuid, rb_timeuuid_allocate);
    rb_define_method(cCassTimeUuid, "initialize", rb_timeuuid_initialize, -1);
    
    // Class methods
    rb_define_singleton_method(cCassTimeUuid, "generate", rb_timeuuid_generate, 0);
    rb_define_singleton_method(cCassTimeUuid, "from_time", rb_timeuuid_from_time, 1);
    
    // Instance methods
    rb_define_method(cCassTimeUuid, "to_s", rb_timeuuid_to_s, 0);
    rb_define_method(cCassTimeUuid, "timestamp", rb_timeuuid_timestamp, 0);
    rb_define_method(cCassTimeUuid, "to_time", rb_timeuuid_to_time, 0);
    rb_define_method(cCassTimeUuid, "==", rb_timeuuid_equal, 1);
    rb_define_method(cCassTimeUuid, "eql?", rb_timeuuid_equal, 1);
    rb_define_method(cCassTimeUuid, "hash", rb_timeuuid_hash, 0);
    rb_define_method(cCassTimeUuid, "<=>", rb_timeuuid_compare, 1);
    rb_define_method(cCassTimeUuid, "cassandra_typed_timeuuid?", rb_timeuuid_is_timeuuid, 0);
}

// Cleanup function (called on extension unload)
void cleanup_timeuuid() {
    if (global_uuid_gen) {
        cass_uuid_gen_free(global_uuid_gen);
        global_uuid_gen = NULL;
    }
}