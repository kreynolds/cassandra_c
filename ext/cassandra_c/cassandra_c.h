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

// ============================================================================
// Ruby Data Type Definitions
// ============================================================================

extern const rb_data_type_t cluster_type;
extern const rb_data_type_t session_type;
extern const rb_data_type_t future_type;
extern const rb_data_type_t prepared_type;
extern const rb_data_type_t statement_type;
extern const rb_data_type_t result_type;

// ============================================================================
// Ruby Class Declarations
// ============================================================================

extern VALUE cCassStatement;
extern VALUE cCassResult;
extern VALUE cCassFuture;
extern VALUE cCassPrepared;

// ============================================================================
// Core Function Declarations
// ============================================================================

// Error handling
void raise_cassandra_error(CassError error, const char* message) __attribute__((noreturn));
void raise_future_error(CassFuture* future, const char* prefix) __attribute__((noreturn));

// Object creation functions
VALUE future_new(CassFuture* future);
VALUE prepared_new(const CassPrepared* prepared);
VALUE statement_new(CassStatement* statement);
VALUE result_new(CassResult* result);

// Value conversion
VALUE cass_value_to_ruby(const CassValue* value);
CassError ruby_value_to_cass_statement(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_value_to_cass_statement_by_name(CassStatement* statement, const char* name, VALUE rb_value);

// Type-specific binding functions
CassError ruby_string_to_cass_text(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_string_to_cass_text_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_string_to_cass_ascii(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_string_to_cass_ascii_by_name(CassStatement* statement, const char* name, VALUE rb_value);
CassError ruby_string_to_cass_blob(CassStatement* statement, size_t index, VALUE rb_value);
CassError ruby_string_to_cass_blob_by_name(CassStatement* statement, const char* name, VALUE rb_value);

// ============================================================================
// Module Initialization Functions
// ============================================================================

void Init_cassandra_c_cluster(VALUE module);
void Init_cassandra_c_session(VALUE module);
void Init_cassandra_c_future(VALUE module);
void Init_cassandra_c_prepared(VALUE module);
void Init_cassandra_c_statement(VALUE module);
void Init_cassandra_c_result(VALUE module);

#endif /* CASSANDRA_C_H */
