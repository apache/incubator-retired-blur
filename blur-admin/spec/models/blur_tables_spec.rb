require 'spec_helper'

describe BlurTables do
  
  before(:each) do
      @client = mock(ThriftClient)
      BlurThriftClient.stub!(:client).and_return(@client)
      @blur_tables = BlurTables.new :table_name => 'blah', :status => "2"
  end
  
  describe "enable" do
    
    before(:each) do
      @table_descr = Blur::TableDescriptor.new :isEnabled => true
    end
    
    it "method sends the signal to enable the table through thrift" do
      @client.should_receive(:enableTable).with('blah')
      @blur_tables.enable.should == true
    end
  end
  
  describe "disable" do
    
    before(:each) do
      @table_descr = Blur::TableDescriptor.new :isEnabled => false
    end
    
    it "method sends the signal to disabl the table through thrift" do
      @blur_tables = BlurTables.new :table_name => 'blah', :status => "1git"
      @client.should_receive(:disableTable).with('blah')
      @blur_tables.disable.should == false
    end
  end  
end
