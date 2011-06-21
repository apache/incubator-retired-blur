class BlurQuery < ActiveRecord::Base
  belongs_to :blur_table

  def shards
    # TODO: Fix shard counting logic, switch to using DB
    return 5
#   begin
#     shard_list = BlurThriftClient.client.shardServerLayout self.table_name
#     shard_list.length
#   rescue => exception
#     puts exception
#     puts "Exception in BlurQueries.shards"
#     nil
#   end
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
