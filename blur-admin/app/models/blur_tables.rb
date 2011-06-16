class BlurTables < ActiveRecord::Base
  require 'blur_thrift_client'
  
  def is_enabled?
    describe_table ? @table_description.isEnabled : false
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
      BlurThriftClient.client.disableTable self.table_name
    ensure
      return self.is_enabled?
    end
  end 

  def destroy underlying=false
    begin
      #TODO: Uncomment line below when ready to delete tables in Blur
      #BlurThriftClient.client.removeTable self.table_name underlying
      return true;
    rescue => exception
      puts exception
      return false;
    end
  end
  
  def table_uri
    describe_table
    @table_description ? @table_description.tableUri : nil
  end
  
  def table_analyzer
    describe_table
    @table_description ? @table_description.analyzerDefinition.fullTextAnalyzerClassName : nil
  end
  
  def schema
    schema_table
    @table_schema ? @table_schema : nil
  end
  
  def server
    list_server
    @server_layout ? @server_layout : nil
  end
  
  def shards
    list_server
    if @server_layout
      hosts = {}
      @server_layout.each do |shard,host|
        hosts[host] = [] unless hosts.has_key? host
        hosts[host] << shard
      end
    end
    hosts ? hosts : nil
  end
  
  private
  
  def describe_table 
    begin
      @table_description = BlurThriftClient.client.describe(self.table_name) unless @table_description
    rescue
      puts "Exception in BlurTable.describe_table"
      return false
    end
  end
  
  def schema_table
    begin
      @table_schema = BlurThriftClient.client.schema(self.table_name).columnFamilies unless @table_schema
    rescue
      puts "Exception in BlurTable.schema_table"
      return false
    end
  end
  
  def list_server
    begin
      @server_layout = BlurThriftClient.client.shardServerLayout(self.table_name) unless @server_layout
    rescue
      puts "Exception in BlurTable.list_server"
      return false
    end
  end
end
