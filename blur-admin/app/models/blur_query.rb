require 'blur_thrift_client'

class BlurQuery < ActiveRecord::Base
  belongs_to :blur_table
  has_one :cluster, :through => :blur_table

  def cancel
    begin
      BlurThriftClient.client.cancelQuery self.blur_table.table_name, self.uuid
      return true
    rescue Exception
      puts "Exception in BlurQueries.cancel:"
      puts $!, $@
      return false
    end
  end

  # rails 3.0 does not allow nested has_one :through relationships
  def zookeeper
    self.blur_table.zookeeper
  end

  def times
    JSON.parse read_attribute(:times)
  end

  def state
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
end
