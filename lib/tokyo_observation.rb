require 'tokyo_tyrant'

class TokyoObservation
  def add(metric_uuid, time, value)
    data = [time.to_i, value].pack('If')
    connection.putcat(key(metric_uuid, time), data)
  end

  def read(metric_uuid, time)
    values = connection.get(key(metric_uuid, time))
    values = deserialize(values) if values
    values
  end

  def deserialize(string)
    [].tap do |data|
      string.unpack('If' * (string.length / 8)).each_slice(2) { |slice| data << slice }
    end
  end

  def delete(metric_uuid, time)
    connection.delete(key(metric_uuid, time))
  end

  def clear
    connection.clear
  end

  def key(metric_uuid, time)
    "#{metric_uuid}_#{time.utc.strftime('%Y%m%d')}"
  end

  def connection
    @connection ||= TokyoTyrant::BDB.new('127.0.0.1', 1979)
  end

  def files
    '/usr/local/var/tokyo-tyrant/test.tcb'
  end
end
