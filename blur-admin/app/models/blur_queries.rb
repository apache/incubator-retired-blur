class BlurQueries < ActiveRecord::Base

  def shards
    begin
      shard_list = BlurThriftClient.client.shardServerLayout self.table_name
      shard_list.length
    rescue => exception
      puts exception
      puts "Exception in BlurQueries.shards"
      nil
    end
  end

  def cancel
    begin
      BlurThriftClient.client.cancelQuery self.table_name, self.uuid.to_i
      return true
    rescue
      puts "Exception in BlurQueries.cancel"
      return false
    end
  end
end
