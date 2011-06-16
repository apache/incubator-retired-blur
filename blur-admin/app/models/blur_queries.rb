class BlurQueries < ActiveRecord::Base
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
