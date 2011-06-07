require File.dirname(__FILE__) + '/../spec_helper'

describe User do
  before(:each) do
    @valid_attributes = {
      :username => "Bob",
      :password => "password",
      :password_confirmation => "password_confirmation",
      :email => "example@example.com"
    }
  end

  it "should be valid" do
    User.create(@valid_attributes)
  end
end
