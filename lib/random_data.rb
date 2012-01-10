require 'uuidtools'
require 'gsl'

class RandomData
  attr_accessor :metrics, :values, :start_at, :end_at, :step_size

  def initialize(options = {})
    options = { :metric_count => 1, :day_count => 1, :samples_per_day => 720 }.merge(options.to_hash)

    print "Generating random data for #{options[:metric_count]} metrics over #{options[:day_count]} days sampled #{options[:samples_per_day]} times per day..."
    STDOUT.flush

    @metrics = []
    @values = []

    options[:metric_count].times { @metrics << UUID.random_create.to_s }

    random_number_generator = GSL::Rng.alloc('gsl_rng_mt19937', Time.now.to_i)

    (options[:day_count] * options[:samples_per_day]).times do
      @values << random_number_generator.exponential(1.0)
    end

    @start_at = Time.at(0)
    @end_at = Time.at(0) + (options[:day_count] * 24 * 60 * 60)
    @step_size = (1 * 24 * 60 * 60) / options[:samples_per_day]

    puts " Done."
  end
end
