require 'mongo'

class MongoObservation
  def add(metric_uuid, time, value)
    data = [time.to_i, value]
    collection.update({'_id' => key(metric_uuid, time)}, {'$push' => {'values' => data}}, :upsert => true)
  end

  def read(metric_uuid, time)
    values = collection.find('_id' => key(metric_uuid, time)).to_a.first
    values = values["values"] if values
    values
  end

  def delete(metric_uuid, time)
    collection.remove({'_id' => key(metric_uuid, time)})
  end

  def clear
    collection.remove
  end

  def key(metric_uuid, time)
    "#{metric_uuid}_#{time.utc.strftime('%Y%m%d')}"
  end

  def connection
    @connection ||= Mongo::Connection.new.db('mongobasher_test')
  end

  def collection
    @collection ||= connection.collection('observations')
  end

  def files
    '/usr/local/var/mongodb/mongobasher_test.*'
  end
end
