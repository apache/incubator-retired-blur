require "spec_helper"
require "cancan/matchers"

describe Ability do
  describe "when not logged in" do
    before(:each) do
      @ability = Ability.new(nil)
    end

    it "can create a user (register)" do
      @ability.should be_able_to :create, :users
    end

    it "can create a user_session (log in)" do
      @ability.should be_able_to :create, :user_sessions 
    end


  end
end
