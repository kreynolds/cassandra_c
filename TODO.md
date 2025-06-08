# CassandraC Production Readiness TODO

This document outlines all features and improvements needed to make CassandraC a production-ready Cassandra driver for Ruby.

## 🏗️ Architecture Strategy

**Native vs Idiomatic Approach**: We follow a two-layer architecture:

1. **CassandraC::Native** - Low-level faithful bindings to the DataStax C/C++ driver
   - Direct exposure of C driver functionality
   - Minimal Ruby abstraction for maximum performance
   - Used as foundation for higher-level wrappers

2. **CassandraC** (Future) - Idiomatic Ruby interface layer
   - Ruby-friendly API with blocks, enumerables, and conventions
   - Built on top of Native layer
   - Convenience methods and syntactic sugar

**Current Priority**: Complete and stabilize the Native layer before building idiomatic wrappers.

## ✅ Recently Completed

### Core Components  
- [x] **Cluster Configuration**: Basic cluster setup with contact points, port, protocol version
- [x] **Session Management**: Connect, disconnect, client ID retrieval
- [x] **Basic Query Execution**: Simple queries and prepared statements
- [x] **Async Operations**: Future-based async support for connect, prepare, execute
- [x] **Data Type Wrappers**: Basic Ruby typed data wrappers for memory management
- [x] **Error Handling**: Robust error propagation with proper memory management
- [x] **Consistency Levels**: Symbol and integer-based consistency level setting
- [x] **Load Balancing**: Round-robin and DC-aware load balancing policies
- [x] **Retry Policies**: Default, fallthrough, and logging retry policies
- [x] **Memory Management**: Proper cleanup with RUBY_TYPED_FREE_IMMEDIATELY

### Parameter Binding (NEW ✅)
- [x] **Prepared Statement Parameter Binding**: 
  - [x] Array-based parameter binding in `prepared.bind([params])`
  - [x] Index-based binding with `statement.bind_by_index(index, value)`
  - [x] Named parameter binding with `statement.bind_by_name(name, value)`
  - [x] Support for string, float, boolean, and nil values
  - [x] Proper error handling and validation

### Development Infrastructure (NEW ✅)
- [x] **Namespace Architecture**: All C driver bindings moved to `CassandraC::Native`
- [x] **Test Organization**: Comprehensive test suite with proper table schemas
- [x] **Error Message Handling**: Centralized `raise_future_error()` helper for consistent error handling
- [x] **Memory Safety**: Fixed corruption issues in error message extraction

## 🚧 Core Driver Features (High Priority)

### Connection & Authentication
- [ ] **SSL/TLS Support**: 
  - [ ] SSL context configuration
  - [ ] Certificate validation options
  - [ ] Client certificate authentication
  - [ ] Custom CA certificate support
- [ ] **Authentication**:
  - [ ] Username/password authentication
  - [ ] SASL authentication mechanisms
  - [ ] Kerberos/GSSAPI support
  - [ ] LDAP authentication
- [ ] **Connection Pooling**:
  - [ ] Configurable connection pool size
  - [ ] Connection health monitoring
  - [ ] Connection recovery strategies
  - [ ] Connection timeout configuration

### Data Types & Value Conversion
- [ ] **Complete Data Type Support**:
  - [x] **Text/varchar/ASCII types** ✅ (Complete with UTF-8 and ASCII validation, multibyte character support)
  - [x] **Integer types** ✅ (Complete with typed wrappers and overflow handling):
    - [x] TinyInt (8-bit, -128 to 127) for TINYINT
    - [x] SmallInt (16-bit, -32,768 to 32,767) for SMALLINT  
    - [x] Int (32-bit, -2,147,483,648 to 2,147,483,647) for INT
    - [x] BigInt (64-bit) for BIGINT
    - [x] VarInt (unlimited precision) for VARINT with large number support
    - [x] Conversion methods: `42.to_cassandra_tinyint`, `1000.to_cassandra_smallint`, etc.
    - [x] Proper parameter binding and result parsing in C extension
    - [x] Arithmetic operations preserve types with overflow wrapping
  - [x] **Floating point and decimal types** ✅ (Complete with arbitrary precision support):
    - [x] Float (32-bit IEEE 754) with `bind_float_by_index/name` methods
    - [x] Double (64-bit IEEE 754) with `bind_double_by_index/name` methods  
    - [x] Decimal (arbitrary precision) with `bind_decimal_by_index/name` methods
    - [x] Conversion methods: `3.14.to_cassandra_float`, `2.718.to_cassandra_double`, `"123.456".to_cassandra_decimal`
    - [x] BigDecimal integration for arbitrary precision decimal arithmetic
    - [x] Varint encoding/decoding for Cassandra DECIMAL compatibility
    - [x] Type-safe arithmetic operations and comparisons
    - [x] Proper parameter binding and result parsing in C extension
  - [x] **Boolean type** ✅ (Complete with comprehensive test coverage):
    - [x] True/false value binding by index and name
    - [x] Array parameter binding with boolean values
    - [x] Null value handling for boolean columns
    - [x] Result parsing returns proper Ruby true/false/nil values
    - [x] Integration with simple and prepared queries
  - [x] **Blob/binary data** ✅ (Complete with proper binary encoding handling):
    - [x] `bind_blob_by_index` and `bind_blob_by_name` methods
    - [x] Binary data storage and retrieval with ASCII-8BIT encoding
    - [x] Support for all binary data types (files, raw bytes, etc.)
    - [x] Proper null and empty data handling
  - [x] **UUID and TimeUUID types** ✅ (Complete with comprehensive test coverage):
    - [x] UUID type with string validation and case-insensitive comparison
    - [x] TimeUUID type with timestamp extraction and chronological ordering
    - [x] `bind_uuid_by_index` and `bind_uuid_by_name` methods
    - [x] `bind_timeuuid_by_index` and `bind_timeuuid_by_name` methods
    - [x] Conversion methods: `"uuid-string".to_cassandra_uuid`, `Time.now.to_cassandra_timeuuid`
    - [x] TimeUUID generation from timestamps with `CassandraC::Types::TimeUuid.from_time`
    - [x] Automatic UUID v4 generation with `CassandraC::Types::Uuid.generate`
    - [x] TimeUUID timestamp extraction with `timeuuid.timestamp`
    - [x] Proper type detection in results (UUID vs TimeUUID based on version)
    - [x] String and IPAddr object support, null handling
  - [ ] Date and time types (date, time, timestamp)
  - [x] **Inet type (IP addresses)** ✅ (Complete with comprehensive IPv4/IPv6 support):
    - [x] `bind_inet_by_index` and `bind_inet_by_name` methods
    - [x] Support for String and IPAddr object input
    - [x] IPv4 and IPv6 address handling
    - [x] Null value support
    - [x] Result parsing returns IP addresses as strings
    - [x] Edge case handling (localhost, compressed IPv6, etc.)
    - [x] Proper validation and error handling for invalid IP addresses
  - [x] **Counter type** ✅ (Complete with comprehensive test coverage):
    - [x] Counter increment and decrement operations
    - [x] Prepared statement support with BigInt parameter binding
    - [x] Multiple counter columns per table
    - [x] Batch counter operations (COUNTER BATCH statements)
    - [x] Large value handling (near int64 limits)
    - [x] Zero and negative counter values
    - [x] Type preservation (counters map to BigInt type)
    - [x] Proper counter table schema requirements (non-counter columns in primary key)
  - [ ] Duration type
- [ ] **Collection Types**:
  - [x] List collections ✅
  - [x] Set collections ✅
  - [x] Map collections ✅
  - [ ] Frozen collections
  - [ ] Nested collections
- [ ] **User Defined Types (UDTs)**:
  - [ ] UDT creation and binding
  - [ ] Nested UDT support
  - [ ] UDT field access
- [ ] **Tuple Types**:
  - [ ] Tuple creation and binding
  - [ ] Tuple element access

### Statement Types & Parameter Binding
- [x] **Simple Statements**: ✅ (complete)
- [x] **Prepared Statements**: ✅ (basic support complete)
  - [x] Parameter binding by index
  - [x] Parameter binding by name  
  - [x] Null value binding
  - [x] Complete integer type binding (tinyint, smallint, int, bigint, varint)
  - [ ] Collection parameter binding
  - [x] Complete numeric type binding (float, double, decimal) ✅
- [x] **Batch Statements** ✅ (Complete with comprehensive batch support):
  - [x] Logged batch support with atomicity guarantees
  - [x] Unlogged batch support for performance
  - [x] Counter batch support for counter operations
  - [x] Mixed statement batching with bound parameters
  - [x] Batch configuration (consistency, serial consistency, timestamp, timeout, idempotent)
  - [x] Async batch execution support
  - [x] Ruby convenience methods (batch(), logged_batch(), unlogged_batch(), counter_batch())
  - [x] Block-based batch builder interface with Batch.build()
  - [x] Comprehensive test coverage for all batch functionality

### Query Features
- [ ] **Paging Support**:
  - [ ] Automatic paging
  - [ ] Manual paging with page state
  - [ ] Page size configuration
  - [ ] Paging state serialization
- [ ] **Token-aware Routing**:
  - [ ] Automatic token map discovery
  - [ ] Token-based query routing
  - [ ] Replica set determination
- [ ] **Tracing Support**:
  - [ ] Query tracing enablement
  - [ ] Trace information retrieval
  - [ ] Custom trace session IDs

### Result Processing
- [ ] **Enhanced Result Handling**:
  - [ ] Column metadata access
  - [ ] Row iteration with proper Ruby enumerables
  - [ ] Result set size and paging info
  - [ ] Column type introspection
- [ ] **Streaming Results**:
  - [ ] Large result set streaming
  - [ ] Memory-efficient row processing
  - [ ] Lazy evaluation support

## 🏗️ Advanced Features (Medium Priority)

### Performance & Monitoring
- [ ] **Metrics Collection**:
  - [ ] Connection metrics
  - [ ] Query performance metrics
  - [ ] Error rate tracking
  - [ ] Latency histograms
- [ ] **Request Routing**:
  - [ ] Token-aware load balancing
  - [ ] Rack-aware routing
  - [ ] Latency-aware routing
- [ ] **Connection Configuration**:
  - [ ] TCP no-delay settings
  - [ ] Keep-alive configuration
  - [ ] Connection timeout tuning
  - [ ] Request timeout configuration

### Advanced Security
- [ ] **Extended SSL Configuration**:
  - [ ] Cipher suite selection
  - [ ] Protocol version selection
  - [ ] Hostname verification options
  - [ ] Certificate revocation checking
- [ ] **Advanced Authentication**:
  - [ ] Custom authenticator support
  - [ ] Token-based authentication
  - [ ] Multi-factor authentication

### Schema Management
- [ ] **Schema Metadata**:
  - [ ] Keyspace introspection
  - [ ] Table metadata retrieval
  - [ ] Column family information
  - [ ] Index information
  - [ ] User-defined type discovery
- [ ] **Schema Change Notifications**:
  - [ ] Schema change event listening
  - [ ] Automatic schema refresh
  - [ ] Schema version tracking

### Advanced Load Balancing
- [ ] **Custom Load Balancing Policies**:
  - [ ] Latency-aware routing
  - [ ] Custom policy implementation interface
  - [ ] Policy composition support
- [ ] **Advanced Retry Policies**:
  - [ ] Custom retry policy implementation
  - [ ] Retry decision logging
  - [ ] Exponential backoff strategies

## 🔧 Developer Experience (Medium Priority)

### Ruby Integration
- [ ] **ActiveRecord Integration**:
  - [ ] ActiveRecord adapter
  - [ ] Migration support
  - [ ] Association mapping
- [ ] **Ruby Idioms**:
  - [ ] Enumerable result sets
  - [ ] Block-based iteration
  - [ ] Method chaining support
  - [ ] Ruby-style naming conventions
- [ ] **Configuration Management**:
  - [ ] YAML configuration file support
  - [ ] Environment variable configuration
  - [ ] Configuration validation

### Testing & Development
- [ ] **Test Utilities**:
  - [ ] Embedded Cassandra support for testing
  - [ ] Mock driver for unit testing
  - [ ] Test data fixtures
- [ ] **Debugging Support**:
  - [ ] Query logging
  - [ ] Performance profiling hooks
  - [ ] Debug mode with verbose output
- [ ] **Documentation**:
  - [ ] Comprehensive API documentation
  - [ ] Usage examples
  - [ ] Migration guides
  - [ ] Performance tuning guide

## 📊 Production Readiness (High Priority)

### Observability
- [ ] **Logging Integration**:
  - [ ] Configurable log levels
  - [ ] Structured logging support
  - [ ] Integration with Ruby logging frameworks
  - [ ] **Server-side Warning System**:
    - [ ] Capture and surface Cassandra server-side warnings
    - [ ] Organized warning categorization (performance, schema, batch size, etc.)
    - [ ] Warning level configuration and filtering
    - [ ] Integration with Ruby logging frameworks for warnings
    - [ ] Warning aggregation and rate limiting
- [ ] **Health Checks**:
  - [ ] Connection health monitoring
  - [ ] Cluster health status
  - [ ] Node availability checks
- [ ] **Monitoring Hooks**:
  - [ ] Custom metrics export
  - [ ] Integration with monitoring systems
  - [ ] Alert-worthy condition detection

### Error Handling & Recovery
- [ ] **Enhanced Error Handling**:
  - [ ] Specific exception types for different errors
  - [ ] Error categorization (retryable vs non-retryable)
  - [ ] Error context preservation
- [ ] **Connection Recovery**:
  - [ ] Automatic reconnection
  - [ ] Circuit breaker pattern
  - [ ] Graceful degradation
- [ ] **Node Failure Handling**:
  - [ ] Automatic node discovery
  - [ ] Failed node detection
  - [ ] Node blacklisting and recovery

### Resource Management
- [ ] **Memory Management**:
  - [ ] Configurable memory limits
  - [ ] Memory usage monitoring
  - [ ] Garbage collection optimization
- [ ] **Thread Safety**:
  - [ ] Thread-safe operations verification
  - [ ] Concurrent access testing
  - [ ] Lock contention minimization
- [ ] **Resource Cleanup**:
  - [ ] Automatic resource deallocation
  - [ ] Connection pool cleanup
  - [ ] Memory leak prevention

## 🔄 Compatibility & Standards (Low Priority)

### Protocol Support
- [ ] **Protocol Version Management**:
  - [ ] Automatic protocol negotiation
  - [ ] Protocol feature detection
  - [ ] Backward compatibility support
- [ ] **CQL Feature Support**:
  - [ ] Full CQL 3.x support
  - [ ] JSON support
  - [ ] Materialized view support
  - [ ] Secondary index support

### Platform Support
- [ ] **Multi-platform Support**:
  - [ ] Windows compatibility
  - [ ] macOS compatibility (ARM64)
  - [ ] Linux distributions support
- [ ] **Ruby Version Support**:
  - [ ] Ruby 3.0+ support
  - [ ] JRuby compatibility
  - [ ] TruffleRuby compatibility

### Standards Compliance
- [ ] **Database Standards**:
  - [ ] DB-API 2.0 compliance where applicable
  - [ ] Standard SQL data types mapping
- [ ] **Security Standards**:
  - [ ] FIPS compliance options
  - [ ] SOC 2 compliance considerations

## 🧪 Testing & Quality Assurance

### Test Coverage
- [ ] **Unit Tests**:
  - [ ] Complete API coverage
  - [ ] Edge case testing
  - [ ] Error condition testing
- [ ] **Integration Tests**:
  - [ ] Multi-node cluster testing
  - [ ] Network partition testing
  - [ ] Load testing
- [ ] **Performance Tests**:
  - [ ] Benchmark suite
  - [ ] Memory usage profiling
  - [ ] Latency measurement

### Quality Metrics
- [ ] **Code Quality**:
  - [ ] Static analysis integration
  - [ ] Code coverage reporting
  - [ ] Performance regression detection
- [ ] **Documentation Quality**:
  - [ ] API documentation completeness
  - [ ] Example code validation
  - [ ] Tutorial accuracy

## 🚀 Deployment & Distribution

### Packaging
- [ ] **Gem Distribution**:
  - [ ] Multi-platform gem builds
  - [ ] Native extension compilation
  - [ ] Dependency management
- [ ] **Container Support**:
  - [ ] Docker image optimization
  - [ ] Kubernetes deployment guides
- [ ] **Cloud Platform Support**:
  - [ ] AWS deployment guides
  - [ ] GCP integration examples
  - [ ] Azure compatibility

### Version Management
- [ ] **Semantic Versioning**:
  - [ ] Clear version policy
  - [ ] Breaking change documentation
  - [ ] Migration guides between versions
- [ ] **Release Process**:
  - [ ] Automated release pipeline
  - [ ] Release notes generation
  - [ ] Security update process

---

## Priority Implementation Order

1. **Phase 1 (Core Stability)**: Complete data types, enhanced error handling, SSL/TLS
2. **Phase 2 (Production Features)**: Batch statements, paging, metrics, monitoring
3. **Phase 3 (Advanced Features)**: Schema management, advanced load balancing, ActiveRecord integration
4. **Phase 4 (Platform Maturity)**: Multi-platform support, comprehensive testing, documentation

This roadmap ensures CassandraC becomes a robust, production-ready Cassandra driver that can compete with drivers in other languages while providing excellent Ruby integration.