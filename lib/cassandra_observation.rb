require 'cassandra'

class CassandraObservation
  def add(metric_uuid, time, value)
    column_family_name = key(metric_uuid, time)

    unless connection.column_families.include? column_family_name
      column_family = Cassandra::ColumnFamily.new
      column_family.keyspace = 'Observations'
      column_family.name = column_family_name
      column_family.comparator_type = "org.apache.cassandra.db.marshal.UTF8Type"
      connection.add_column_family column_family
    end

    connection.insert(column_family_name, time.to_i.to_s, { 'value' => value.to_s })
  end

  def read(metric_uuid, time)
    column_family_name = key(metric_uuid, time)

    if connection.column_families.include? column_family_name
      values = connection.get_range_single(column_family_name)
      [].tap { |data| values.each { |key, value| data << [key, value['value']] } }
    else
      nil
    end
  end

  def delete(metric_uuid, time)
    column_family_name = key(metric_uuid, time)

    if connection.column_families.include? column_family_name
      connection.drop_column_family(key(metric_uuid, time))
    end
  end

  def clear
    connection.column_families.each_key do |column_family|
      connection.drop_column_family column_family
    end
  end

  def key(metric_uuid, time)
    key = "#{metric_uuid.gsub(/-/, '_')}_#{time.utc.strftime('%Y%m%d')}"
  end

  def connection
    @connection ||= Cassandra.new('Observations', '127.0.0.1:9160', :retries => 5, :timeout => 5, :connect_timeout => 5)
  end

  def files
    '/usr/local/var/lib/cassandra/data/Observations/*'
  end
end
