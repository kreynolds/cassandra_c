# frozen_string_literal: true

require "bundler/gem_tasks"
require "minitest/test_task"

Minitest::TestTask.create

require "standard/rake"

require "rake/extensiontask"

task build: :compile

GEMSPEC = Gem::Specification.load("cassandra_c.gemspec")

Rake::ExtensionTask.new("cassandra_c", GEMSPEC) do |ext|
  ext.lib_dir = "lib/cassandra_c"
end

# Task for running tests with coverage
desc "Run tests with SimpleCov coverage reporting"
task :test_with_coverage do
  # Compile first in a separate step to avoid SimpleCov interference
  Rake::Task[:compile].invoke

  # Now run tests with coverage in a clean environment
  sh "COVERAGE=true bundle exec ruby -Ilib:test -e \"require 'minitest/autorun'; Dir['test/**/*test*.rb'].each { |f| require_relative f }\""
end

# Full development workflow - compile, test, lint
task default: %i[clobber compile test standard]

# CI workflow with coverage
desc "Run CI workflow with coverage reporting"
task ci: %i[clobber compile test_with_coverage standard]
