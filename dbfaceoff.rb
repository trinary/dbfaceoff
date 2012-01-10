#!/usr/bin/env ruby
$:.unshift File.expand_path('../lib', __FILE__)

require 'rubygems'

# set up gems listed in the Gemfile
ENV['BUNDLE_GEMFILE'] ||= File.expand_path('../Gemfile', __FILE__)
require 'bundler/setup' if File.exists?(ENV['BUNDLE_GEMFILE'])

require 'options'
require 'random_data'
require 'profiler'

options = Options.new
data = RandomData.new(options)
Profiler.new(data).run(options[:data_stores], options[:operations])
