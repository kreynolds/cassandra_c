#ifndef CASSANDRA_C_H
#define CASSANDRA_C_H 1

#include "ruby.h"
#include "cassandra.h"

// Declare the CassandraC module
extern VALUE mCassandraC;

// Declare the global consistency map
extern VALUE consistency_map;

// Declare function that raises Cassandra errors
void raise_cassandra_error(CassError error, const char* message) __attribute__((noreturn));

// Define the ClusterWrapper structure
typedef struct {
    CassCluster* cluster;
} ClusterWrapper;
extern const rb_data_type_t cluster_type;

// Define the FutureWrapper structure
typedef struct {
    CassFuture* future;
} FutureWrapper;
extern const rb_data_type_t future_type;

// Define the SessionWrapper structure
typedef struct {
    CassSession* session;
} SessionWrapper;
extern const rb_data_type_t session_type;

// Define the PreparedStatementWrapper structure
typedef struct {
    const CassPrepared* prepared;
} PreparedWrapper;
extern const rb_data_type_t prepared_type;

// Define the StatementWrapper structure
typedef struct {
    CassStatement* statement;
} StatementWrapper;
extern const rb_data_type_t statement_type;

// Define the ResultWrapper structure
typedef struct {
    CassResult* result;
} ResultWrapper;
extern const rb_data_type_t result_type;

// Row structure not needed anymore, we yield arrays directly

// Declare the CassandraC::Error exception
extern VALUE rb_eCassandraError;

// Declare classes
extern VALUE cCassStatement;
extern VALUE cCassResult;

// Function declarations
VALUE future_new(CassFuture*);
VALUE prepared_new(const CassPrepared*);
VALUE statement_new(CassStatement*);
VALUE result_new(CassResult*);
VALUE cass_value_to_ruby(const CassValue*);

#endif /* CASSANDRA_C_H */
