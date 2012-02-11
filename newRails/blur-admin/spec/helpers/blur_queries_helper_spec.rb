require 'spec_helper'
describe BlurQueryHelper do
  describe "format title" do
    it "returns the nothing when it is less than 20 chars" do
      format_title("Simple").should == ''
    end    
    
    it "returns a string with the ' +' with a line break" do
      format_title("Not Very Simple +And Kind Of Long").should == "Not Very Simple<br />+And Kind Of Long"
    end
  end
  
  describe "print value" do
    it "returns the default message when given conditional evaluates to false" do 
      print_value(false).should == 'Not Available'
    end
    
    it "returns the conditional when no block is given" do 
      print_value('hello').should == 'hello'
    end 
    
    it "executes the block when the conditional is false" do 
      print_value(true){'hello'}.should == 'hello'
    end
  end
end
