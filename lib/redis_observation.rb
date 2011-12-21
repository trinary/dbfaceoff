require 'redis'

class RedisObservation
  def add(metric_uuid, time, value)
    data = [time.to_i, value].inspect
    connection.rpush(key(metric_uuid, time), data)
  end

  def read(metric_uuid, time)
    connection.lrange(key(metric_uuid, time), 0, -1)
  end

  def delete(metric_uuid, time)
    connection.del(key(metric_uuid, time))
  end

  def clear
    connection.flushdb
  end

  def key(metric_uuid, time)
    "#{metric_uuid}_#{time.utc.strftime('%Y%m%d')}"
  end

  def connection
    @connection ||= Redis.new
  end

  def files
    '/usr/local/var/db/redis/dump.rdb'
  end
end
