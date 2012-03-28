require 'spec_helper'
describe ApplicationHelper do  
  describe "pluralize without count" do
    it "returns the singular if given a 1 or '1'" do 
      pluralize_no_count(1, 'rail').should == 'rail'
      pluralize_no_count('1', 'rail').should == 'rail'
    end

    it 'should return the system defined plural if given a numbe larger than 1' do
      pluralize_no_count(2, 'rail').should == 'rails'
    end 

    it 'should return the given plural if given a numbe larger than 1' do
      pluralize_no_count(2, 'rail', 'not rail').should == 'not rail'
    end 
  end
end