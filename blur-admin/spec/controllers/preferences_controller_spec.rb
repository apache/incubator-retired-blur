require 'spec_helper'

describe PreferencesController do
  describe "actions" do
    before(:each) do
      # Universal Setup
      setup_tests
      
      # Preference for the current user
      @preference = FactoryGirl.create :preference
      User.stub!(:find).and_return @user
      Preference.stub(:find_by_pref_type_and_user_id).and_return(@preference)
    end

    describe "update" do
      it "should find the preference" do
        Preference.should_receive(:find_by_pref_type_and_user_id).with('column', @user.id.to_s)
        @preference.stub!(:try)
        put :update, :user_id => @user.id, :pref_type => 'column', :format => :json
      end

      it "should update the preference" do
        Preference.should_receive(:find_by_pref_type_and_user_id).with('column', @user.id.to_s)
        @preference.should_receive(:try).with(:update_attributes, :value => ['newCol'])
        put :update, :user_id => @user.id, :pref_type => 'column', :value => ['newCol'], :format => :json
      end

      it "should update the preference name and value when it is a zookeeper" do
        Preference.should_receive(:find_by_pref_type_and_user_id).with('zookeeper', @user.id.to_s)
        @preference.should_receive(:try).with(:update_attributes,
                                              {:value => 'test', :name => 'test'})
        put :update, :user_id => @user.id, :pref_type => 'zookeeper',
                      :value => 'test', :name => 'test', :format => :json
      end

      it "should render a blank json object" do
        @preference.stub!(:try)
        put :update, :user_id => @user.id, :pref_type => 'column', :format => :json
        response.body.should == {}.to_json
      end
    end
  end
end
