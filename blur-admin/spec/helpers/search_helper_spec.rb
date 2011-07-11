require 'spec_helper'

# Specs in this file have access to a helper object that includes
# the SearchHelper. For example:
#
# describe SearchHelper do
#   describe "string concat" do
#     it "concats two strings with spaces" do
#       helper.concat_strings("this","that").should == "this that"
#     end
#   end
# end
describe SearchHelper do
  describe "is_valid_search?" do
    before :each do
      @valid_search = Factory.stub :search
      @invalid_search = Factory.stub :search
      @blur_table = Factory.stub :blur_table
      
      
    end
    
    it "returns true when the search is a subset of the table schema" do 
      is_valid_search?(@valid_search).should == true
    end    
    
    it "returns false when the search is not a subset of the table schema" do
      is_valid_search?(@invalid_search).should == false
    end
  end
end
