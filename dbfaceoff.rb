#!/usr/bin/env ruby
$:.unshift File.expand_path('../lib', __FILE__)

require 'rubygems'

# set up gems listed in the Gemfile
ENV['BUNDLE_GEMFILE'] ||= File.expand_path('../Gemfile', __FILE__)
require 'bundler/setup' if File.exists?(ENV['BUNDLE_GEMFILE'])

require 'optparse'

options = {}

optparse = OptionParser.new do |opts|
  opts.banner = "Usage: dbfaceoff.rb [options]"
  options[:tokyo] = nil
  options[:mongo] = nil
  options[:redis] = nil
  options[:cassandra] = nil

  opts.on('--[no-]tokyo', 'Run tests with Tokyo Tyrant') do |value|
    options[:tokyo] = value
  end

  opts.on('--[no-]mongo', 'Run tests with MongoDB') do |value|
    options[:mongo] = value
  end

  opts.on('--[no-]redis', 'Run tests with Redis') do |value|
    options[:redis] = value
  end

  opts.on('--[no-]cassandra', 'Run tests with Cassandra') do |value|
    options[:cassandra] = value
  end

  opts.on('--[no-]read', 'Run read tests') do |value|
    options[:read] = value
  end

  opts.on('--[no-]write', 'Run write tests') do |value|
    options[:write] = value
  end

  opts.on('--[no-]delete', 'Run delete tests') do |value|
    options[:delete] = value
  end

  opts.on('--metric-count COUNT', Integer, 'Run with COUNT metrics (default: 1)') do |count|
    options[:metric_count] = count
  end

  opts.on('--day-count COUNT', Integer, 'Run with COUNT days of data (default: 1)') do |count|
    options[:day_count] = count
  end

  opts.on('--samples-per-day COUNT', Integer, 'Run with COUNT samples per day of data (default: 720)') do |count|
    options[:samples_per_day] = count
  end

  opts.on('-h', '--help', 'Display this screen') do
    puts opts
    exit
  end
end

optparse.parse!

def gather_boolean_options(keys, options)
  [].tap do |result|
    keys.each { |key| result << key if options[key] }

    if result.empty?
      result.concat keys
      keys.each { |key| result.delete(key) if options[key] == false }
    end
  end
end

data_stores = gather_boolean_options [:tokyo, :mongo, :redis, :cassandra], options
operations = gather_boolean_options [:read, :write, :delete], options

require 'random_data'
data = RandomData.new(options)

require 'profiler'
Profiler.new(data).run(data_stores, operations)
