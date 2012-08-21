require 'spec_helper'

describe ApplicationController do
  describe "actions" do
    before(:each) do
      # Set up the stubs and variables
      setup_variables_and_stubs
      # Set the ability (leave current user unset for testing purposes)
      set_ability

      activate_authlogic

      @user_session = UserSession.new({:username => @user.username, :password => @user.password, :commit => "Log In"})
      @user_session.stub!(:user).and_return @user
      controller.stub!(:current_user_session).and_return @user_session
    end

    it "Current user should grab the current user session and set the current user" do
      controller.should_receive(:current_user_session)
      controller.current_user
      assigns(:current_user).should == @user
    end

    it "help should render the help menu with the given tab" do 
      get 'help', :tab => 'search'
      assigns(:tab).should == 'search'
      response.should render_template :partial => "layouts/_help_menu"
    end

    it "the license action should set the @license variable" do
      License.stub!(:first).and_return 'License'
      controller.license
      assigns(:license).should == 'License'
    end
  end

  describe 'Enable Authorization: Visiting a page without authorization' do
    it "without a current_user" do
      get 'help', :tab => 'search'
      response.should redirect_to(login_path)
    end

    it "with a current user and no ability to view the root page redirects to logout_url" do 
      @user = FactoryGirl.create :user, :roles => []
      controller.stub!(:current_user).and_return(@user)
      get 'help', :tab => 'search'
      response.should redirect_to(logout_url)
    end

    it "with a current user and ability to view the root page redirects to index zookeeper" do 
      @user = FactoryGirl.create :user, :roles => []
      controller.stub!(:current_user).and_return(@user)
      controller.stub!(:can?).and_return(true)
      get 'help', :tab => 'search'
      response.should redirect_to(root_url)
    end
  end
end