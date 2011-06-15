require 'spec_helper'

describe BlurTables do
  before(:each) do
      @client = mock(ThriftClient)
      BlurThriftClient.stub!(:client).and_return(@client)
      @blur_tables = BlurTables.new :table_name => 'blah'
  end

  describe "is_enabled" do
    before(:each) do
      @table_descr = Blur::TableDescriptor.new :isEnabled => true
      @client.stub!(:describe).with('blah').and_return(@table_descr)
    end    
    it "returns true when the table is enabled" do
      @blur_tables.is_enabled?.should be true
    end
  end
end
