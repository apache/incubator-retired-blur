require 'blur_thrift_client'

class BlurQuery < ActiveRecord::Base
  belongs_to :blur_table

  def cancel
    begin
      BlurThriftClient.client.cancelQuery self.blur_table.table_name, self.uuid
      return true
    rescue Exception
      puts "Exception in BlurQueries.cancel"
      puts $!, $@
      return false
    end
  end

  # rails 3.0 does not allow nested has_one :through relationships
  def zookeeper
    self.shards.first.zookeeper
  end

  def cluster
    self.shards.first.cluster
  end
  
  def shards
    self.blur_table.shards
  end
end
