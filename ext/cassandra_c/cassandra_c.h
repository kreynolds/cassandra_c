#ifndef CASSANDRA_C_H
#define CASSANDRA_C_H 1

#include "ruby.h"
#include "cassandra.h"

/*
 * CassandraC Ruby Extension
 * 
 * This header file defines the main structures, types, and function declarations
 * for the CassandraC Ruby extension that provides Ruby bindings for the 
 * DataStax Cassandra C/C++ driver.
 */

// ============================================================================
// Module and Exception Declarations
// ============================================================================

extern VALUE mCassandraC;
extern VALUE mCassandraCNative;
extern VALUE rb_eCassandraError;

// TimeUuid class
extern VALUE cCassTimeUuid;
void Init_cassandra_c_timeuuid(VALUE module);
void cleanup_timeuuid();
CassUuid rb_timeuuid_get_cass_uuid(VALUE timeuuid_obj);
VALUE rb_timeuuid_from_cass_uuid(CassUuid uuid);

// ============================================================================
// Global Configuration
// ============================================================================

extern VALUE consistency_map;

// ============================================================================
// Wrapper Structures for Cassandra Types
// ============================================================================

typedef struct {
    CassCluster* cluster;
} ClusterWrapper;

typedef struct {
    CassSession* session;
} SessionWrapper;

typedef struct {
    CassFuture* future;
} FutureWrapper;

typedef struct {
    const CassPrepared* prepared;
} PreparedWrapper;

typedef struct {
    CassStatement* statement;
} StatementWrapper;

typedef struct {
    CassResult* result;
} ResultWrapper;

typedef struct {
    CassBatch* batch;
} BatchWrapper;

// ============================================================================
// Ruby Data Type Definitions
// ============================================================================

extern const rb_data_type_t cluster_type;
extern const rb_data_type_t session_type;
extern const rb_data_type_t future_type;
extern const rb_data_type_t prepared_type;
extern const rb_data_type_t statement_type;
extern const rb_data_type_t result_type;
extern const rb_data_type_t batch_type;

// ============================================================================
// Ruby Class Declarations
// ============================================================================

extern VALUE cCassStatement;
extern VALUE cCassResult;
extern VALUE cCassFuture;
extern VALUE cCassPrepared;
extern VALUE cCassBatch;

// ============================================================================
// Core Function Declarations
// ============================================================================

// Error handling
void raise_cassandra_error(CassError error, const char* message) __attribute__((noreturn));
void raise_future_error(CassFuture* future, const char* prefix) __attribute__((noreturn));

// Shared utility functions
CassConsistency ruby_value_to_consistency(VALUE consistency);

// Object creation functions
VALUE future_new(CassFuture* future);
VALUE prepared_new(const CassPrepared* prepared);
VALUE statement_new(CassStatement* statement);
VALUE result_new(CassResult* result);
VALUE batch_new(CassBatch* batch);

// Value conversion
VALUE cass_value_to_ruby(const CassValue* value);
CassError ruby_value_to_cass_statement(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_statement_by_name(CassStatement* statement, const char* name, VALUE rb_value);

// Type-hinted value conversion
CassError ruby_value_to_cass_statement_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE type_hint);
CassError ruby_value_to_cass_statement_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE type_hint);

// Type-specific binding functions
CassError ruby_string_to_cass_text(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_string_to_cass_text_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_string_to_cass_ascii(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_string_to_cass_ascii_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_string_to_cass_blob(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_string_to_cass_blob_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_inet(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_inet_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_float(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_float_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_double(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_double_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_decimal(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_decimal_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_uuid(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_uuid_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_timeuuid(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_timeuuid_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_date(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_date_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_time(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_time_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_timestamp(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_timestamp_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_list(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_list_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_set(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_set_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_value_to_cass_map(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_map_by_name(CassStatement* statement, const char* name, VALUE rb_value);

// Type-hinted collection binding functions
CassError ruby_value_to_cass_list_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE element_type);
CassError ruby_value_to_cass_list_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE element_type);
CassError ruby_value_to_cass_set_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE element_type);
CassError ruby_value_to_cass_set_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE element_type);
CassError ruby_value_to_cass_map_with_type(CassStatement* statement, size_t index, VALUE rb_value, VALUE key_type, VALUE value_type);
CassError ruby_value_to_cass_map_with_type_by_name(CassStatement* statement, const char* name, VALUE rb_value, VALUE key_type, VALUE value_type);

// ============================================================================
// Module Initialization Functions
// ============================================================================

void Init_cassandra_c_cluster(VALUE module);
void Init_cassandra_c_session(VALUE module);
void Init_cassandra_c_future(VALUE module);
void Init_cassandra_c_prepared(VALUE module);
void Init_cassandra_c_statement(VALUE module);
void Init_cassandra_c_result(VALUE module);
void Init_cassandra_c_batch(VALUE module);

#endif /* CASSANDRA_C_H */
