# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CassandraC is a Ruby gem that provides Ruby bindings for the Datastax C/C++ Cassandra Driver. It's designed to offer high-performance access to Cassandra databases from Ruby applications through a native C extension.

## Development Environment Setup

### Prerequisites

- Ruby >= 3.0.0
- Cassandra C/C++ driver (`libcassandra` and development headers)
- Docker and Docker Compose (for testing with a Cassandra instance)

### Installation

The Cassandra C/C++ driver must be installed before you can build the gem:

```bash
# On MacOS
brew install cassandra-cpp-driver

# On Debian/Ubuntu
apt-get install libuv1 libuv1-dev libcassandra libcassandra-dev
```

## Commands

### Build and Install

```bash
# Install dependencies
bundle install

# Build the C extension
bundle exec rake compile

# Build the gem
bundle exec rake build

# Install the gem locally
bundle exec rake install
```

### Testing

```bash
# Start Cassandra container for testing
docker-compose up -d

# Run all tests
bundle exec rake test

# Run native layer tests only
bundle exec ruby -Itest test/native/test_session.rb

# Run a specific test
bundle exec ruby -Itest test/native/test_session.rb -n test_connects_and_disconnects

# Run specific test suites
bundle exec rake test TEST="test/native/**/*.rb"
```

### Code Quality

```bash
# Run standardrb linter
bundle exec rake standard

# Fix linting issues
bundle exec rake standard:fix
```

### Complete Build Process

```bash
# Complete build, including compilation, tests and linting
bundle exec rake
```

## Architecture

The gem follows a two-layer architecture:

### Native Layer (`CassandraC::Native`)
Low-level faithful bindings to the DataStax C/C++ driver:
- **`CassandraC::Native::Cluster`**: Cluster configuration and connection management
- **`CassandraC::Native::Session`**: Query execution and session management  
- **`CassandraC::Native::Future`**: Async operation handling
- **`CassandraC::Native::Statement`**: CQL statement representation
- **`CassandraC::Native::Prepared`**: Prepared statement management
- **`CassandraC::Native::Result`**: Query result processing

### Idiomatic Layer (`CassandraC`) - Future
Ruby-friendly interface built on top of the Native layer:
- Enumerable result sets
- Block-based iteration
- Method chaining
- Ruby naming conventions

**Current Focus**: Completing and stabilizing the Native layer before building idiomatic wrappers.

### Extension Structure

- `cassandra_c.c/h`: Core module definitions and initialization
- `cluster.c`: Cluster connection and configuration
- `session.c`: Session management and query execution
- `future.c`: Async operation handling
- `prepared.c`: Prepared statement operations
- `result.c`: Result processing
- `statement.c`: Statement creation and binding
- `value.c`: Data type conversions between Ruby and Cassandra

### Memory Management

The C extension uses Ruby's typed data to properly manage memory and ensure resources are freed when no longer needed, using the RUBY_TYPED_FREE_IMMEDIATELY flag to immediately free Cassandra resources when Ruby objects are garbage collected.

## Workflow Memories

### Feature Development Process
- When starting a feature session, ensure that main is up to date, then check out a new branch, do the work, then open a PR once everything is done.

### Complete Feature Implementation Workflow
**ALWAYS** follow this complete checklist for feature implementation:

1. **Implementation Phase**
   - Research existing patterns and implementations
   - Implement the feature in C extension and/or Ruby code
   - Add function declarations to headers as needed

2. **Testing Phase** 
   - Create comprehensive test suite covering the feature
   - **Review tests for duplication and debugging artifacts**
   - Remove excessive comments, verbose explanations, and debugging code
   - Ensure tests are focused, clean, and maintainable
   - Run full test suite to ensure no regressions

3. **Documentation Phase**
   - Update TODO.md to mark completed features
   - Add comprehensive examples to EXAMPLES.md
   - Update any relevant documentation files
   - Review documentation for clarity and completeness

4. **Quality Assurance Phase**
   - Run linting and code quality checks (`bundle exec rake standard`)
   - Run complete build process (`bundle exec rake`)
   - Fix any linting or compilation issues

5. **Git Workflow Phase**
   - Stage and commit changes with descriptive commit message
   - Push branch to remote repository
   - **ALWAYS create a pull request using `gh pr create`**
   - Include comprehensive PR description with summary and test plan

### Documentation Update Requirements
For any new feature, ALWAYS update:
- **TODO.md**: Mark feature as complete with ✅
- **EXAMPLES.md**: Add comprehensive usage examples
- **Test coverage**: Ensure clean, focused tests without debugging artifacts
- **Any relevant documentation**: README, architecture docs, etc.

### Test Quality Standards
- Remove excessive comments that explain obvious behavior
- Eliminate verbose debugging output and temporary code
- Consolidate redundant test cases
- Use concise Ruby idioms (e.g., `&:to_i` instead of verbose blocks)
- Focus on essential test scenarios without over-testing edge cases
- Ensure tests are maintainable and easy to understand