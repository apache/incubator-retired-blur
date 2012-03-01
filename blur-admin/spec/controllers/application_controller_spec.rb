require 'spec_helper'

describe ApplicationController do
  describe "actions" do
    describe 'Application methods' do
      before(:each) do
        @user = FactoryGirl.create :user
        @ability = Ability.new @user
        @ability.stub!(:can?).and_return(true)
        controller.stub!(:current_ability).and_return(@ability)
      end

      it "Current user should grab the current user session and set the current user" do
        @session = mock(UserSession, :user => @user)
        controller.stub!(:current_user_session).and_return @session
        controller.should_receive(:current_user_session)
        get 'help', :tab => 'search'
        assigns(:current_user).should == @user
      end

      it "help should render the help menu with the given tab" do 
        get 'help', :tab => 'search'
        assigns(:tab).should == 'search'
        response.should render_template :partial => "layouts/_help_menu"
      end
    end

    describe 'Enable Authorization: Visiting a page without authorization' do
      it "without a current_user" do
        get 'help', :tab => 'search'
        response.should redirect_to(login_path)
      end

      it "with a current user and no ability to view the root page redirects to logout_url" do 
        @user = FactoryGirl.create :user, :roles => '0'
        controller.stub!(:current_user).and_return(@user)
        get 'help', :tab => 'search'
        response.should redirect_to(logout_url)
      end

      it "with a current user and ability to view the root page redirects to index zookeeper" do 
        @user = FactoryGirl.create :user, :roles => '0'
        controller.stub!(:current_user).and_return(@user)
        controller.stub!(:can?).and_return(true)
        get 'help', :tab => 'search'
        response.should redirect_to(root_url)
      end
    end
  end
end