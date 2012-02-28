require 'spec_helper'

describe PreferencesController do
  before(:each) do
    @user = FactoryGirl.create :user
    @ability = Ability.new @user
    @ability.stub!(:can?).and_return(true)
    @preference = FactoryGirl.create :preference
    controller.stub!(:current_user).and_return(@user)
    controller.stub!(:current_ability).and_return(@ability)
    User.stub!(:find).and_return @user
    Preference.stub(:find_by_pref_type_and_user_id).and_return(@preference)
  end

  describe "update" do
    it "should find the preference" do
      Preference.should_receive(:find_by_pref_type_and_user_id).with('column', @user.id.to_s)
      @preference.stub!(:try)
      put :update, :user_id => @user.id, :pref_type => 'column'
    end

    it "should update the preference" do
      Preference.should_receive(:find_by_pref_type_and_user_id).with('column', @user.id.to_s)
      @preference.should_receive(:try).with(:update_attributes, :value => ['newCol'])
      put :update, :user_id => @user.id, :pref_type => 'column', :value => ['newCol']
    end

    it "should render nothing" do
      @preference.stub!(:try)
      put :update, :user_id => @user.id, :pref_type => 'column'
      response.body.should be_blank
    end
  end
end
