require 'optparse'

class Options
  def initialize
    parse
  end

  def [](key)
    @options[key]
  end

  def to_hash
    @options.dup
  end

  private

  def parse
    @options = {}

    option_parser = OptionParser.new do |parser|
      parser.banner = "Usage: dbfaceoff.rb [options]"
      @options[:tokyo] = nil
      @options[:mongo] = nil
      @options[:redis] = nil
      @options[:cassandra] = nil

      parser.on('--[no-]tokyo', 'Run tests with Tokyo Tyrant') do |value|
        @options[:tokyo] = value
      end

      parser.on('--[no-]mongo', 'Run tests with MongoDB') do |value|
        @options[:mongo] = value
      end

      parser.on('--[no-]redis', 'Run tests with Redis') do |value|
        @options[:redis] = value
      end

      parser.on('--[no-]cassandra', 'Run tests with Cassandra') do |value|
        @options[:cassandra] = value
      end

      parser.on('--[no-]read', 'Run read tests') do |value|
        @options[:read] = value
      end

      parser.on('--[no-]write', 'Run write tests') do |value|
        @options[:write] = value
      end

      parser.on('--[no-]delete', 'Run delete tests') do |value|
        @options[:delete] = value
      end

      parser.on('--metric-count COUNT', Integer, 'Run with COUNT metrics (default: 1)') do |count|
        @options[:metric_count] = count
      end

      parser.on('--day-count COUNT', Integer, 'Run with COUNT days of data (default: 1)') do |count|
        @options[:day_count] = count
      end

      parser.on('--samples-per-day COUNT', Integer, 'Run with COUNT samples per day of data (default: 720)') do |count|
        @options[:samples_per_day] = count
      end

      parser.on('-h', '--help', 'Display this screen') do
        puts parser
        exit
      end
    end

    option_parser.parse!

    @options[:data_stores] = gather_boolean_options([:tokyo, :mongo, :redis, :cassandra])
    @options[:operations] = gather_boolean_options([:read, :write, :delete])

    self
  end

  def gather_boolean_options(keys)
    [].tap do |result|
      keys.each { |key| result << key if @options[key] }

      if result.empty?
        result.concat keys
        keys.each { |key| result.delete(key) if @options[key] == false }
      end
    end
  end
end
