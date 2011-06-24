class BlurTable < ActiveRecord::Base
  require 'blur_thrift_client'

  has_many :blur_queries

  def schema
    if self.table_schema
      JSON.parse self.table_schema
    else
      return nil
    end
  end

  def definition
    if self.server
      JSON.parse self.server
    else
      return nil
    end
  end

  def is_enabled?
    self.status == 2
  end

  def enable
    begin
      BlurThriftClient.client.enableTable self.table_name
    ensure
      return self.is_enabled?
    end
  end

  def disable
    begin
      #BlurThriftClient.client.disableTable self.table_name
    ensure
      return self.is_enabled?
    end
  end 

  def destroy underlying=false
    begin
      #TODO: Uncomment line below when ready to delete tables in Blur
      #BlurThriftClient.client.removeTable self.table_name underlying
      return true;
    rescue
      puts "Exception in BlurTable.destroy"
      return false;
    end
  end
end
