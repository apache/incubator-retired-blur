require 'spec_helper'

describe Zookeeper do
  describe "blur_queries" do
    before :each do
      @table = Factory.stub :blur_table
      @query = Factory.stub :blur_query
      @zoo = Zookeeper.new
      @table.stub(:blur_queries).and_return([@query])
      @zoo.stub(:blur_tables).and_return([@table, @table])
    end
    
    it "uses the tables to return all of the queries associated with them" do
      @zoo.blur_queries.should == [@query, @query]
    end
  
  end 
end
