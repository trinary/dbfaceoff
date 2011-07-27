#!/usr/bin/env ruby

require 'rubygems'

# set up gems listed in the Gemfile
ENV['BUNDLE_GEMFILE'] ||= File.expand_path('../Gemfile', __FILE__)

require 'bundler/setup' if File.exists?(ENV['BUNDLE_GEMFILE'])

require 'mongo'
require 'tokyo_tyrant'
#require 'kyototycoon'
require 'uuidtools'
require 'gsl'
require 'benchmark'

#METRIC_COUNT = 100
METRIC_COUNT = 2
OBSERVATION_DAY_COUNT = 365
SAMPLES_PER_DAY = 720

class MongoObservation
  def self.add(metric_uuid, time, value)
    data = [time.to_i, value]

    collection.update({'_id' => key(metric_uuid, time)}, {'$push' => {'values' => data}}, :upsert => true)
  end

  def self.read(metric_uuid, time)
    collection.find('_id' => key(metric_uuid, time))
  end

  def self.delete(metric_uuid, time)
    collection.remove({'_id' => key(metric_uuid, time)})
  end

  def self.clear
    collection.remove
  end

  def self.key(metric_uuid, time)
    key = "#{metric_uuid}_#{time.utc.strftime('%Y%m%d')}"
  end

  def self.connection
    @@connection ||= Mongo::Connection.new.db('mongobasher_test')
  end

  def self.collection
    @@collection ||= connection.collection('observations')
  end
end

class TokyoObservation
  def self.add(metric_uuid, time, value)
    data = [time.to_i, value].pack('If')

    connection.putcat(key(metric_uuid, time), data)
  end

  def self.read(metric_uuid, time)
    connection.get(key(metric_uuid, time))
  end

  def self.delete(metric_uuid, time)
    connection.delete(key(metric_uuid, time))
  end

  def self.clear
    connection.clear
  end

  def self.key(metric_uuid, time)
    key = "#{metric_uuid}_#{time.utc.strftime('%Y%m%d')}"
  end

  def self.connection
    @@connection ||= TokyoTyrant::BDB.new('127.0.0.1', 1979)
  end
end

#class KyotoObservation
  #def self.add(metric_uuid, time, value)
    #key = "#{metric_uuid}_#{time.utc.strftime('%Y%m%d')}"
    #data = [time.to_i, value].pack('If')

    #connection.set(key, data)
  #end

  #def self.connection
    #@@connection ||= KyotoTycoon.new('127.0.0.1', 1977)
  #end
#end

metrics = []
values = []

METRIC_COUNT.times do
  metrics << UUID.random_create.to_s
end

random_number_generator = GSL::Rng.alloc('gsl_rng_mt19937', Time.now.to_i)

(OBSERVATION_DAY_COUNT * SAMPLES_PER_DAY).times do
  values  << random_number_generator.exponential(1.0)
end

start_at = Time.now.to_i - (OBSERVATION_DAY_COUNT * 24 * 60 * 60)
end_at = Time.now.to_i - (1 * 24 * 60 * 60)
step_size = (1 * 24 * 60 * 60) / SAMPLES_PER_DAY

MongoObservation.clear
TokyoObservation.clear

Benchmark.bm(15) do |x|
  x.report('mongo add') do
    metrics.each do |metric|
      i = 0

      (start_at..end_at).step(step_size) do |timestamp|
        time = Time.at(timestamp)
        value = values[i]

        MongoObservation.add(metric, time, value)

        i += 1
      end
    end
  end

  x.report('tokyo add') do
    metrics.each do |metric|
      i = 0

      (start_at..end_at).step(step_size) do |timestamp|
        time = Time.at(timestamp)
        value = values[i]

        TokyoObservation.add(metric, time, value)

        i += 1
      end
    end
  end

  x.report('mongo read') do
    metrics.each do |metric|
      (start_at..end_at).step(86400) do |timestamp|
        time = Time.at(timestamp)

        MongoObservation.read(metric, time)
      end
    end
  end

  x.report('tokyo read') do
    metrics.each do |metric|
      (start_at..end_at).step(86400) do |timestamp|
        time = Time.at(timestamp)

        TokyoObservation.read(metric, time)
      end
    end
  end

  x.report('mongo delete') do
    metrics.each do |metric|
      (start_at..end_at).step(86400) do |timestamp|
        time = Time.at(timestamp)

        MongoObservation.delete(metric, time)
      end
    end
  end

  x.report('tokyo delete') do
    metrics.each do |metric|
      (start_at..end_at).step(86400) do |timestamp|
        time = Time.at(timestamp)

        TokyoObservation.delete(metric, time)
      end
    end
  end

  #x.report('kyoto') do
    #metrics.each do |metric|
      #i = 0

      #(start_at..end_at).step(step_size) do |timestamp|
        #time = Time.at(timestamp)
        #value = values[i]

        #KyotoObservation.add(metric, time, value)

        #i += 1
      #end
    #end
  #end
end
