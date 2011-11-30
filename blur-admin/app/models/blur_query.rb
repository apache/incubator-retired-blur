require 'blur_thrift_client'

class BlurQuery < ActiveRecord::Base
  belongs_to :blur_table
  has_one :cluster, :through => :blur_table

  def cancel
    begin
      BlurThriftClient.client(blur_table.zookeeper.blur_urls).cancelQuery self.blur_table.table_name, self.uuid
      return true
    rescue Exception => e
      logger.error "Exception in BlurQueries.cancel:"
      logger.error e
      return false
    end
  end

  # rails 3.0 does not allow nested has_one :through relationships
  def zookeeper
    self.blur_table.zookeeper
  end

  def times
    JSON.parse(read_attribute(:times)).each do |shard, value|
      value.reject! {|type, value| type == 'setCpuTime' or type == 'setRealTime'}
    end
  end

  def state_str
    case read_attribute(:state)
      when 0 then "Running"
      when 1 then "Interrupted"
      when 2 then "Complete"
      else nil
    end
  end

  def complete
    self.complete_shards / self.total_shards.to_f
  end
  
  def self.where_zookeeper(zookeeper_id)
    joins(:blur_table => :cluster).where(:blur_table =>{:clusters => {:zookeeper_id => zookeeper_id}}).includes(:blur_table).order("created_at DESC")
  end
  
  def self.filter_on_time_range(range)
    where(:updated_at => range)
  end
end
