require 'spec_helper'

describe BlurTable do
  
  before(:each) do
    @client = mock(ThriftClient)
    BlurThriftClient.stub!(:client).and_return(@client)
    @table = Factory.create :blur_table
  end
  
  describe "enable " do
    it "method sends the message to enable the table through thrift" do
      @client.should_receive(:enableTable).with @table.table_name
      @table.enable 'test:40000'
    end
  end
  
  describe "disable" do
    it "should send the message to disable the table through thrift" do
      #@table = BlurTable.new :table_name => 'blah', :status => "1git"
      @client.should_receive(:disableTable).with(@table.table_name)
      @table.disable 'test:40000'
    end
  end  
  
  describe "schema" do
    it "returns the table schema in a ruby hash, with hosts as keys and array of shards as values" do
      @table.hosts.should == JSON.parse( @table.server )
      @table.hosts.keys.each {|host| host.should match  /Host/}
      @table.hosts.values.flatten.each {|shard| shard.should match /shard/}
    end

    it "sorts the columns and column families alphabetically" do
      @unsorted_table = Factory.create :blur_table,
        :table_name => 'test_table',
        :table_schema =>      [
                                {
                                  "name" => 'ColumnFamily2',
                                  "columns" => [
                                    {"name" => 'Column2A'},
                                    {"name" => 'Column2B'},
                                    {"name" => 'Column2C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily1',
                                  "columns" => [
                                    {"name" => 'Column1A'},
                                    {"name" => 'Column1B'},
                                    {"name" => 'Column1C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily3',
                                  "columns" => [
                                    {"name" => 'Column3A'},
                                    {"name" => 'Column3B'},
                                    {"name" => 'Column3C'}
                                  ]
                                }
                              ].to_json

      @sorted_table = Factory.create :blur_table,
        :table_name => 'test-table',
        :table_schema =>      [
                                {
                                  "name" => 'ColumnFamily1',
                                  "columns" => [
                                    {"name" => 'Column1A'},
                                    {"name" => 'Column1B'},
                                    {"name" => 'Column1C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily2',
                                  "columns" => [
                                    {"name" => 'Column2A'},
                                    {"name" => 'Column2B'},
                                    {"name" => 'Column2C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily3',
                                  "columns" => [
                                    {"name" => 'Column3A'},
                                    {"name" => 'Column3B'},
                                    {"name" => 'Column3C'}
                                  ]
                                }
                              ].to_json


        @unsorted_table.schema.should == @sorted_table.schema
    end

    it "sorts the column families by an optionally supplied block" do
      @unsorted_table = Factory.create :blur_table,
        :table_name => 'test_table',
        :table_schema =>      [
                                {
                                  "name" => 'ColumnFamily1',
                                  "columns" => [
                                    {"name" => 'Column1A'},
                                    {"name" => 'Column1B'},
                                    {"name" => 'Column1C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily2',
                                  "columns" => [
                                    {"name" => 'Column2A'},
                                    {"name" => 'Column2B'},
                                    {"name" => 'Column2C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily3',
                                  "columns" => [
                                    {"name" => 'Column3A'},
                                    {"name" => 'Column3B'},
                                    {"name" => 'Column3C'}
                                  ]
                                }
                              ].to_json

      @reverse_sort_table = Factory.create :blur_table,
        :table_name => 'test_table',
        :table_schema =>      [
                                {
                                  "name" => 'ColumnFamily3',
                                  "columns" => [
                                    {"name" => 'Column3A'},
                                    {"name" => 'Column3B'},
                                    {"name" => 'Column3C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily2',
                                  "columns" => [
                                    {"name" => 'Column2A'},
                                    {"name" => 'Column2B'},
                                    {"name" => 'Column2C'}
                                  ]
                                },
                                {
                                  "name" => 'ColumnFamily1',
                                  "columns" => [
                                    {"name" => 'Column1A'},
                                    {"name" => 'Column1B'},
                                    {"name" => 'Column1C'}
                                  ]
                                }
                              ].to_json


      @reverse_sort_table.schema.should == @unsorted_table.schema {|a, b| a['name'] <=> b['name']}
    end

    it "returns nil when the server has not been populated" do
      blur_table = BlurTable.new
      blur_table.server.should be nil
    end
  end
end
