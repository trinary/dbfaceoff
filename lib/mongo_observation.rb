require 'mongo'

class MongoObservation
  def add(metric_uuid, time, value)
    data = [time.to_i, value]
    collection.update({'_id' => key(metric_uuid, time), 'date' => format_date(time)}, {'$push' => {'values' => data}}, :upsert => true)
  end

  def read(metric_uuid, time)
    values = collection.find('_id' => key(metric_uuid, time), 'date' => format_date(time)).to_a.first
    values = values['values'] if values
    values
  end

  def delete(metric_uuid, time)
    collection.remove({'metric_uuid' => metric_uuid, 'date' => format_date(time)})
    collection.remove({'_id' => key(metric_uuid, time)})
  end

  def clear
    collection.remove
  end

  def key(metric_uuid, time)
    "#{metric_uuid}_#{format_date(time)}"
  end

  def format_date(time)
    time.utc.strftime('%Y%m%d')
  end

  def connection
    @connection ||= Mongo::Connection.new.db('test')
  end

  def collection
    @collection ||= connection.collection('observations')
  end

  def files
    ['/Users/adam/mongo/data/db/shard1/test.*', '/Users/adam/mongo/data/db/shard2/test.*']
  end
end
