require 'spec_helper'

describe BlurTable do
  
  before(:each) do
      @client = mock(ThriftClient)
      BlurThriftClient.stub!(:client).and_return(@client)
      @table = BlurTable.new :table_name =>    'blah',
                                    :status =>        "2",
                                    :table_schema =>  "{\"Host1:101\":[\"shard-001\", \"shard-002\", \"shard-003\"], \"Host2:102\":[\"shard-004\", \"shard-005\", \"shard-006\"]}"
  end
  
  describe "enable " do
    
    it "method sends the message to enable the table through thrift" do
      @client.should_receive(:enableTable).with('blah')
      @table.enable.should == true
    end
  end
  
  describe "disable" do
    it "should send the message to disable the table through thrift" do
      pending "Uncommenting line in model to enable disabling"
      @table = BlurTable.new :table_name => 'blah', :status => "1git"
      @client.should_receive(:disableTable).with('blah')
    end
  end  
  
  describe "schema" do
    it "returns the table schema in a ruby hash, with hosts as keys and array of shards as values" do
      @table.schema.should == JSON.parse( @table.table_schema )
      @table.schema.keys.each {|host| host.should match  /Host/}
      @table.schema.values.flatten.each {|shard| shard.should match /shard/}
    end

    it "returns nil when the table_schema has not been populated" do
      blur_table = BlurTable.new
      blur_table.schema.should be nil
    end
  end
end
