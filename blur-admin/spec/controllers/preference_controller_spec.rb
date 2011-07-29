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
    
    it "should set the new values on the found or created preference" do
      Preference.stub(:find_or_create_by_user_id_and_pref_type_and_name).and_return(@preference)
      controller.stub!(:current_user).and_return @user
      @preference.should_receive(:value=).with(["column1", "column2"].to_json())
      post :save, :columns => ["column1", "column2"]
      response.body.should be_blank
    end
  end
  
  describe "save filters" do
    before do
      @preference = Preference.new
    end
    
    it "should render a blank response when given the correct variables" do
      Preference.stub(:find_or_create_by_user_id_and_pref_type_and_name).and_return(@preference)
      post :save_filters, :created_at_time => '5',
                          :super_query_on => true,
                          :running => true, 
                          :interrupted => false, 
                          :refresh_period => 60
      response.body.should be_blank
    end
    
    it "should set the new values on the found or created preference" do
      @filters =  {:created_at_time => 5,
                   :super_query_on  => true,
                   :running         => true,
                   :interrupted     => false,
                   :refresh_period  => 60}

      Preference.stub(:find_or_create_by_user_id_and_pref_type_and_name).and_return(@preference)
      controller.stub!(:current_user).and_return @user
      @preference.should_receive(:value=).with(@filters.to_json())
      post :save_filters, :created_at_time => 5,
                          :super_query_on => true,
                          :running => true, 
                          :interrupted => false, 
                          :refresh_period => 60
      response.body.should be_blank
    end
  end

end
