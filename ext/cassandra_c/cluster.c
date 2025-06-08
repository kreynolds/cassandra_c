#include "cassandra_c.h"

// Memory management for Cluster
static void cluster_free(void* ptr) {
    ClusterWrapper* wrapper = (ClusterWrapper*)ptr;
    if (wrapper->cluster != NULL) {
        cass_cluster_free(wrapper->cluster);
    }
    xfree(wrapper);
}

// Define the Ruby data type for Cluster
const rb_data_type_t cluster_type = {
    .wrap_struct_name = "CassCluster",
    .function = {
        .dmark = NULL,
        .dfree = cluster_free,
        .dsize = NULL,
    },
    .data = NULL,
    .flags = RUBY_TYPED_FREE_IMMEDIATELY
};

// Allocation function for Cluster
static VALUE rb_cluster_allocate(VALUE klass) {
    ClusterWrapper* wrapper = ALLOC(ClusterWrapper);
    wrapper->cluster = cass_cluster_new();
    if (!wrapper->cluster) {
        rb_raise(rb_eRuntimeError, "Failed to create CassCluster");
    }
    return TypedData_Wrap_Struct(klass, &cluster_type, wrapper);
}

// Initialize method for Session (no arguments)
static VALUE rb_cluster_initialize(VALUE self) {
    // No initialization required beyond allocation
    return self;
}

static VALUE rb_cluster_set_contact_points(VALUE self, VALUE contact_points) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    Check_Type(contact_points, T_STRING);
    CassError error = cass_cluster_set_contact_points(wrapper->cluster, StringValueCStr(contact_points));
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set contact points: %s", cass_error_desc(error));
    }
    return self;
}

static VALUE rb_cluster_set_port(VALUE self, VALUE port) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    CassError error = cass_cluster_set_port(wrapper->cluster, NUM2INT(port));
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set port: %s", cass_error_desc(error));
    }
    return self;
}

static VALUE rb_cluster_set_protocol_version(VALUE self, VALUE protocol_version) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    CassError error = cass_cluster_set_protocol_version(wrapper->cluster, NUM2INT(protocol_version));
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set protocol version: %s", cass_error_desc(error));
    }
    return self;
}

static VALUE rb_cluster_set_num_threads_io(VALUE self, VALUE num_threads) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    CassError error = cass_cluster_set_num_threads_io(wrapper->cluster, NUM2UINT(num_threads));
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set number of IO threads: %s", cass_error_desc(error));
    }
    return self;
}

static VALUE rb_cluster_set_queue_size_io(VALUE self, VALUE queue_size) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    CassError error = cass_cluster_set_queue_size_io(wrapper->cluster, NUM2UINT(queue_size));
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set IO queue size: %s", cass_error_desc(error));
    }
    return self;
}

static VALUE rb_cluster_set_local_address(VALUE self, VALUE address) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    Check_Type(address, T_STRING);
    CassError error = cass_cluster_set_local_address(wrapper->cluster, StringValueCStr(address));
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set local address: %s", cass_error_desc(error));
    }
    return self;
}

// Global hash map for consistency lookups
VALUE consistency_map;

static VALUE rb_cluster_set_consistency(VALUE self, VALUE consistency) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    
    CassConsistency consistency_value = ruby_value_to_consistency(consistency);
    
    CassError error = cass_cluster_set_consistency(wrapper->cluster, consistency_value);
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set consistency level: %s", cass_error_desc(error));
    }
    return self;
}

static VALUE rb_cluster_set_load_balance_round_robin(VALUE self) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    
    cass_cluster_set_load_balance_round_robin(wrapper->cluster);
    
    return self;
}

static VALUE rb_cluster_set_load_balance_dc_aware(VALUE self, VALUE local_dc) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    
    Check_Type(local_dc, T_STRING);
    
    // Use 0 for used_hosts_per_remote_dc and false for allow_remote_dcs_for_local_cl
    // to avoid the deprecation warning. This means no remote DCs will be used.
    CassError error = cass_cluster_set_load_balance_dc_aware(
        wrapper->cluster,
        StringValueCStr(local_dc),
        0,  // Don't use any hosts per remote DC (avoid deprecation warning)
        cass_false  // Don't allow remote DCs for local consistency levels
    );
    
    if (error != CASS_OK) {
        rb_raise(rb_eRuntimeError, "Failed to set DC-aware load balancing policy: %s", cass_error_desc(error));
    }
    
    return self;
}

// Helper functions to create different retry policies

static CassRetryPolicy* create_default_retry_policy() {
    return cass_retry_policy_default_new();
}

static CassRetryPolicy* create_fallthrough_retry_policy() {
    return cass_retry_policy_fallthrough_new();
}

static CassRetryPolicy* create_logging_retry_policy(CassRetryPolicy* child_policy) {
    return cass_retry_policy_logging_new(child_policy);
}

// Custom Ruby object to hold the retry policy setting
static VALUE rb_cluster_set_default_retry_policy(VALUE self) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    
    CassRetryPolicy* policy = create_default_retry_policy();
    cass_cluster_set_retry_policy(wrapper->cluster, policy);
    cass_retry_policy_free(policy);
    
    return self;
}

static VALUE rb_cluster_set_fallthrough_retry_policy(VALUE self) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    
    CassRetryPolicy* policy = create_fallthrough_retry_policy();
    cass_cluster_set_retry_policy(wrapper->cluster, policy);
    cass_retry_policy_free(policy);
    
    return self;
}

static VALUE rb_cluster_set_logging_retry_policy(VALUE self, VALUE child_policy_type) {
    ClusterWrapper* wrapper;
    TypedData_Get_Struct(self, ClusterWrapper, &cluster_type, wrapper);
    
    Check_Type(child_policy_type, T_SYMBOL);
    
    CassRetryPolicy* child_policy = NULL;
    ID policy_id = SYM2ID(child_policy_type);
    
    if (policy_id == rb_intern("default")) {
        child_policy = create_default_retry_policy();
    } else if (policy_id == rb_intern("fallthrough")) {
        child_policy = create_fallthrough_retry_policy();
    } else {
        rb_raise(rb_eArgError, "Invalid retry policy type: %s", RSTRING_PTR(rb_sym2str(child_policy_type)));
    }
    
    CassRetryPolicy* logging_policy = create_logging_retry_policy(child_policy);
    cass_cluster_set_retry_policy(wrapper->cluster, logging_policy);
    
    // Free policies after setting
    cass_retry_policy_free(logging_policy);
    cass_retry_policy_free(child_policy);
    
    return self;
}

// Initialize the consistency map
static void init_consistency_map() {
    // Create hash and register with GC to prevent it from being collected
    consistency_map = rb_hash_new();
    rb_gc_register_address(&consistency_map);
    
    // Map symbols to their enum values
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("any")), INT2NUM(CASS_CONSISTENCY_ANY));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("one")), INT2NUM(CASS_CONSISTENCY_ONE));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("two")), INT2NUM(CASS_CONSISTENCY_TWO));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("three")), INT2NUM(CASS_CONSISTENCY_THREE));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("quorum")), INT2NUM(CASS_CONSISTENCY_QUORUM));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("all")), INT2NUM(CASS_CONSISTENCY_ALL));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("local_quorum")), INT2NUM(CASS_CONSISTENCY_LOCAL_QUORUM));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("each_quorum")), INT2NUM(CASS_CONSISTENCY_EACH_QUORUM));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("serial")), INT2NUM(CASS_CONSISTENCY_SERIAL));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("local_serial")), INT2NUM(CASS_CONSISTENCY_LOCAL_SERIAL));
    rb_hash_aset(consistency_map, ID2SYM(rb_intern("local_one")), INT2NUM(CASS_CONSISTENCY_LOCAL_ONE));
    
    // Freeze the hash to prevent modification
    rb_hash_freeze(consistency_map);
}

void Init_cassandra_c_cluster(VALUE module) {
    // Initialize the consistency map
    init_consistency_map();
    
    VALUE cCluster = rb_define_class_under(module, "Cluster", rb_cObject);
    
    rb_define_alloc_func(cCluster, rb_cluster_allocate);
    rb_define_method(cCluster, "initialize", rb_cluster_initialize, 0);
    rb_define_method(cCluster, "contact_points=", rb_cluster_set_contact_points, 1);
    rb_define_method(cCluster, "port=", rb_cluster_set_port, 1);
    rb_define_method(cCluster, "protocol_version=", rb_cluster_set_protocol_version, 1);
    rb_define_method(cCluster, "num_threads_io=", rb_cluster_set_num_threads_io, 1);
    rb_define_method(cCluster, "queue_size_io=", rb_cluster_set_queue_size_io, 1);
    rb_define_method(cCluster, "local_address=", rb_cluster_set_local_address, 1);
    rb_define_method(cCluster, "consistency=", rb_cluster_set_consistency, 1);
    
    // Load balancing policies
    rb_define_method(cCluster, "use_round_robin_load_balancing", rb_cluster_set_load_balance_round_robin, 0);
    rb_define_method(cCluster, "use_dc_aware_load_balancing", rb_cluster_set_load_balance_dc_aware, 1);
    
    // Retry policies
    rb_define_method(cCluster, "use_default_retry_policy", rb_cluster_set_default_retry_policy, 0);
    rb_define_method(cCluster, "use_fallthrough_retry_policy", rb_cluster_set_fallthrough_retry_policy, 0);
    rb_define_method(cCluster, "use_logging_retry_policy", rb_cluster_set_logging_retry_policy, 1);
}
