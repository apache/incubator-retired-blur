require 'spec_helper'

describe BlurTables do
  
  before(:each) do
      @client = mock(ThriftClient)
      BlurThriftClient.stub!(:client).and_return(@client)
      @blur_tables = BlurTables.new :table_name =>    'blah',
                                    :status =>        "2",
                                    :table_schema =>  "{\"Host1:101\":[\"shard-001\", \"shard-002\", \"shard-003\"], \"Host2:102\":[\"shard-004\", \"shard-005\", \"shard-006\"]}"
  end
  
  describe "enable " do
    
    before(:each) do
      @table_descr = Blur::TableDescriptor.new :isEnabled => true
    end
    
    it "method sends the message to enable the table through thrift" do
      @client.should_receive(:enableTable).with('blah')
      @blur_tables.enable.should == true
    end
  end
  
  describe "disable" do
    
    before(:each) do
      @table_descr = Blur::TableDescriptor.new :isEnabled => false
    end
    
    it "method sends the message to disable the table through thrift" do
      @blur_tables = BlurTables.new :table_name => 'blah', :status => "1git"
      @client.should_receive(:disableTable).with('blah')
      @blur_tables.disable.should == false
    end
  end  
  
  describe "schema" do
    it "returns the table schema in a ruby hash, with hosts as keys and array of shards as values" do
      @blur_tables.schema.should == JSON.parse( @blur_tables.table_schema )
      @blur_tables.schema.keys.each {|host| host.should match  /Host/}
      @blur_tables.schema.values.flatten.each {|shard| shard.should match /shard/}
    end
  end
end
