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
      Preference.stub(:find_or_create_by_user_id_and_pref_type_and_name).and_return(@preference)
      post :save, :columns => ["column1", "column2"]
      response.body.should be_blank
    end
    
    it "should create a new preference when one does not exist" do
      Preference.stub(:find_or_create_by_user_id_and_pref_type_and_name).and_return(@preference)
      controller.stub!(:current_user).and_return @user
      @preference.should_receive(:value=).with(["column1", "column2"].to_json())
      post :save, :columns => ["column1", "column2"]
      response.body.should be_blank
    end
  end

end
