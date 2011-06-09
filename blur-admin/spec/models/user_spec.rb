require File.dirname(__FILE__) + '/../spec_helper'

describe User do
  it "should be valid" do
    user = User.new(:username => 'bob',
                    :email => 'bob@example.com',
                    :password => 'password',
                    :password_confirmation => 'password')
    user.should be_valid
  end
end
