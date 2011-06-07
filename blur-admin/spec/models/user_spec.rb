require File.dirname(__FILE__) + '/../spec_helper'

describe User do
  it "should be valid" do
    User.new.should be_valid
  end
end
