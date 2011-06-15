class BlurTables < ActiveRecord::Base
  require 'blur_thrift_client'
  
  def is_enabled?
    describe_table
    @table_description.isEnabled
  end

  def enable
    BlurThriftClient.client.enableTable self.table_name
  end
  
  def disable
    BlurThriftClient.client.disableTable self.table_name
  end 
  
  def table_uri
    describe_table
    @table_description.tableUri
  end
  
  def table_analyzer
    describe_table
    @table_description.analyzerDefinition.fullTextAnalyzerClassName
  end
  
  def schema
    schema_table
    @table_schema
  end
  
  def server
    list_server
    @server_layout
  end
  
  def shards
    hosts = {}
    self.server.each do |shard,host|
      hosts[host] = [] unless hosts.has_key? host
      hosts[host] << shard
    end
    hosts
  end
  
  private
  
  def describe_table 
    @table_description = BlurThriftClient.client.describe(self.table_name) unless @table_description
  end
  
  def schema_table
    @table_schema = BlurThriftClient.client.schema(self.table_name).columnFamilies unless @table_schema
  end
  
  def list_server
    @server_layout = BlurThriftClient.client.shardServerLayout(self.table_name)
  end
end
