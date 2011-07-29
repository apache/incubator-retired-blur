require 'spec_helper'
describe SearchHelper do
  describe "is_valid_search?" do
    before :each do
      @blur_table = Factory.stub :blur_table
    end
    
    it "returns true when the search is a subset of the table schema" do 
      @valid_search = Factory.stub :search
      is_valid_search?(@valid_search).should == true
    end    
    
    it "returns false when the search is not a subset of the table schema" do
      @invalid_search = Factory.stub :search, :columns => ["column_blah_ack", "column_blah_blah"]
      is_valid_search?(@invalid_search).should == false
    end
  end
end
