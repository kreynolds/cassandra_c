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

# Full development workflow - compile, test, lint
task default: %i[clobber compile test standard]

# CI workflow
desc "Run CI workflow"
task ci: %i[clobber compile test standard]
