class BlurTable < ActiveRecord::Base
  require 'blur_thrift_client'

  belongs_to :cluster
  has_many :blur_queries
  has_many :searches
  has_one :zookeeper, :through => :cluster

  # Returns a map of host => [shards] of all hosts/shards associated with the table
  def hosts
    if self.server
      JSON.parse self.server
    else
      return nil
    end

  end

  def schema
    if self.table_schema
      # sort columns, and then sort column families
      if block_given?
        Hash[(JSON.parse self.table_schema)['columnFamilies'].each {|k, v| v.sort!}.sort &Proc.new]
      else
        Hash[(JSON.parse self.table_schema)['columnFamilies'].each {|k, v| v.sort!}.sort]
      end
    else
      return nil
    end
  end

  def num_shards
    self.schema.values.flatten.count if self.schema
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

  def <=> (other)
    if other.is_enabled? == self.is_enabled?
      return self.table_name <=> other.table_name
    elsif self.is_enabled?
      return -1
    else
      return 1
    end
  end
end
