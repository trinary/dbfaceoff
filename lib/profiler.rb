class Profiler
  def initialize(data)
    @data = data
  end

  def run(data_stores, operations)
    data_stores.each do |data_store|
      puts "------------------------------------------"
      puts ""

      require "#{data_store}_observation"
      data_store_name = data_store.to_s.chars.to_a[0].upcase + data_store.to_s[1..-1]
      puts data_store_name

      store = Kernel.const_get(data_store_name + 'Observation').new
      store.clear

      run_write(store) if operations.include?(:write)
      run_read(store) if operations.include?(:read)
      run_delete(store) if operations.include?(:delete)

      puts ""
    end
  end

  def with_timing(title)
    puts ""
    puts "  #{title}:"

    # reset iostat
    `iostat -dI 2>&1 > /dev/null`

    start_time = Time.now
    result = yield
    end_time = Time.now
    elapsed_time = end_time.to_f - start_time.to_f

    puts "    #{'%.3f' % elapsed_time} seconds (#{'%.3f' % (result / elapsed_time)} requests/second)"
    puts ""
    puts `iostat -dI`
    puts ""
  end

  def report_file_size(file_pattern)
    file_patterns = [file_pattern].flatten
    total = 0

    file_patterns.each do |file_pattern|
      total += Dir[file_pattern].inject(0) { |sum, filename| sum += File.size(filename) }
    end

    puts "    Size on disk: #{total} bytes"
  end

  def run_write(store)
    with_timing 'write' do
      count = 0

      @data.metrics.each do |metric|
        i = 0

        timestamp = @data.start_at
        while timestamp < @data.end_at
          time = Time.at(timestamp)
          value = @data.values[i]

          unless value.nil?
            store.add(metric, time, value)
            i += 1
          end
          timestamp += @data.step_size
        end

        count += i
      end

      puts "    Wrote #{count} observations."
      count
    end

    report_file_size store.files
  end

  def run_read(store)
    with_timing 'read' do
      count = 0

      @data.metrics.each do |metric|
        timestamp = @data.start_at
        while timestamp < @data.end_at do
          time = Time.at(timestamp)
          output = store.read(metric, time)
          if !output.nil? && !output.empty?
            #puts ""
            #puts output.inspect
            #puts ""
            count += output.count
          end
          timestamp += 86400
        end
      end

      puts "    Read #{count} observations."
      count
    end
  end

  def run_delete(store)
    with_timing 'delete' do
      count = 0

      @data.metrics.each do |metric|
        timestamp = @data.start_at
        while timestamp < @data.end_at do
          time = Time.at(timestamp)
          store.delete(metric, time)
          count += 1
          timestamp += 86400
        end
      end

      count
    end
  end
end
