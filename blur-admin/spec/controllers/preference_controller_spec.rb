require 'spec_helper'

describe PreferenceController do
  before do
    @ability = Ability.new User.new
    @ability.stub!(:can?).and_return(true)
    @user = User.new :id => 1
    controller.stub!(:current_user).and_return(@user)
    controller.stub!(:current_ability).and_return(@ability)
  end
  
  describe "save" do
    before do
      @preference = Preference.new
    end
    
    it "should render a blank response when given the correct variables" do
      Preference.stub(:find_by_user_id).and_return(@preference)
      post :save, :columns => ["column1", "column2"]
      response.body.should be_blank
    end
    
    it "should create a new preference when one does not exist" do
      controller.stub!(:current_user).and_return @user
      Preference.stub(:find_by_user_id).and_return nil
      Preference.stub(:create).and_return @preference
      Preference.should_receive(:create).with(:name => "column", :pref_type => "column", :user_id => @user.id)
      @preference.should_receive(:value=).with(["column1", "column2"].to_json())
      post :save, :columns => ["column1", "column2"]
      response.body.should be_blank
    end
  end

end
