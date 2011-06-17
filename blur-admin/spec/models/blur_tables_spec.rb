require 'spec_helper'

describe BlurTables do
  
  before(:each) do
      @client = mock(ThriftClient)
      BlurThriftClient.stub!(:client).and_return(@client)
      @blur_tables = BlurTables.new :table_name => 'blah'
  end

  describe "table description" do
    
    before(:each) do
      @table_analyzer = Blur::AnalyzerDefinition.new :fullTextAnalyzerClassName => "blah"
      @table_descr = Blur::TableDescriptor.new :isEnabled => true, :tableUri => "blah", :analyzerDefinition => @table_analyzer
      @client.stub!(:describe).with('blah').and_return(@table_descr)
    end   
     
    it "method is_enabled returns true when the table is enabled" do
      @blur_tables.is_enabled?.should be true
    end
    
    it "method table_uri returns the proper uri" do
      @blur_tables.table_uri.should == "blah"
    end
    
    it "method table_analyzer returns the proper analyzer" do
      @blur_tables.table_analyzer.should == "blah"
    end
    
    it "throws an exception when the returned table is not valid" do
      @client.stub!(:describe).with('blah').and_raise(Exception)
      lambda{ @blur_tables.table_uri }.should raise_exception
    end
  end
  
  describe "schema" do
    
    before(:each) do
      @table_schema = Blur::Schema.new :columnFamilies => {:family => "tree"}
      @client.stub!(:schema).with('blah').and_return(@table_schema)
    end
    
    it "given a specific schema checks that it returns the formatted hash" do
      @server_layout = {'a_shard' => 'a_host'}
      @returned_host = {'a_host'=>['a_shard']}
      @client.stub!(:shardServerLayout).and_return(@server_layout)
      @blur_tables.shards.should == @returned_host
    end
    
    it "method returns the proper schema" do
      @blur_tables.schema.should == {:family => "tree"}
    end
    
    it "throws an exception when the returned schema is not valid" do
      @client.stub!(:schema).with('blah').and_raise(Exception)
      lambda{ @blur_tables.schema }.should raise_exception
    end
  end
  
  describe "server" do
    
    it "method returns the proper server list" do
      @client.stub!(:shardServerLayout).with('blah').and_return("blah")
      @blur_tables.server.should == "blah"
    end
    
    it "throws an exception when the returned server layout is not valid" do
      @client.stub!(:shardServerLayout).with('blah').and_raise(Exception)
      lambda{ @blur_tables.server }.should raise_exception
    end
    
  end
  
  describe "enable" do
    
    before(:each) do
      @table_descr = Blur::TableDescriptor.new :isEnabled => true
    end
    
    it "method sends the signal to enable the table through thrift" do
      @client.stub!(:describe).and_return(@table_descr)
      @blur_tables.stub!(:table_name).and_return('blah')
      @client.should_receive(:enableTable).with('blah')
      @blur_tables.enable.should == true
    end
  end
  
  describe "disable" do
    
    before(:each) do
      @table_descr = Blur::TableDescriptor.new :isEnabled => false
    end
    
    it "method sends the signal to disabl the table through thrift" do
      @client.stub!(:describe).and_return(@table_descr)
      @blur_tables.stub!(:table_name).and_return('blah')
      @client.should_receive(:disableTable).with('blah')
      @blur_tables.disable.should == false
    end
  end  
end
