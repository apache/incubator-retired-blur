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
  end
  
  describe "schema" do
    
    before(:each) do
      @table_schema = Blur::Schema.new :columnFamilies => {:family => "tree"}
      @client.stub!(:schema).with('blah').and_return(@table_schema)
    end
    
    it "given a schema check that shards returns the formatted hash" do
      @table_server = {'a_shard' => 'a_host'}
      @returned_host = {'a_host'=>['a_shard']}
      @blur_tables.stub!(:server).and_return(@table_server)
      @blur_tables.shards.should == @returned_host
    end
    
    it "method returns the proper schema" do
      @blur_tables.schema.should == {:family => "tree"}
    end
    
  end
  
  describe "server" do
    
    before(:each) do
      @client.stub!(:shardServerLayout).with('blah').and_return("blah")
    end
    
    it "method returns the proper server list" do
      @blur_tables.server.should == "blah"
    end
    
  end
end
