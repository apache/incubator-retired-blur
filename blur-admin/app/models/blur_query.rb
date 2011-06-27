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
end
