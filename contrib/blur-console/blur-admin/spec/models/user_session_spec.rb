require 'spec_helper'

describe UserSession do
  before :each do
    activate_authlogic
    @user_session = UserSession.new
  end

  describe "to_key" do
    it "returns nil when it is a new record " do
      @user_session.stub!(:new_record?).and_return(true)
      @user_session.to_key.should be_nil
    end

    it "sends its primary key when it is a new record " do
      @user_session.stub!(:new_record?).and_return(false)
      @user_session.stub_chain(:class, :primary_key).and_return 1
      @user_session.should_receive(:send).with 1
      @user_session.to_key
    end
  end 

  describe "persisted?" do
    it "always returns false" do
      @user_session.persisted?.should be_false
    end
  end 
end