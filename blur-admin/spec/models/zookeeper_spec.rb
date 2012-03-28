require 'spec_helper'

describe Zookeeper do
  describe "blur_queries" do
    before :each do
      @table = FactoryGirl.create_list :blur_table, 2
      @query = FactoryGirl.create_list :blur_query, 2
      @table.each do |table|
        table.stub!(:blur_queries).and_return(@query)
      end
      @zoo = FactoryGirl.create :zookeeper
      @zoo.stub(:blur_tables).and_return(@table)
    end
    
    it "uses the tables to return all of the queries associated with them" do
      @zoo.blur_queries.should == [@query, @query].flatten
    end
  end 
end
