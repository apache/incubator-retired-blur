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
end

/#
These are tests ripped from zookeeper and they test the logic behind current zookeeper
describe "with persisted zookeepers" do
        describe "with no previous zookeeper" do
          it "should set the current_zookeeper to be the first zookeeper found" do
            get :show_current, nil, nil
            assigns(:current_zookeeper).should == @zookeeper
            session[:current_zookeeper_id].should == @zookeeper.id
          end
        end
        describe "with a valid pre-existing current_zookeeper" do
          it "should set the previous zookeeper to be the current_zookeeper" do
            old_zookeeper = FactoryGirl.create :zookeeper
            Zookeeper.should_receive(:find_by_id).with(old_zookeeper.id).and_return(old_zookeeper)
            get :show_current, nil, :current_zookeeper_id => old_zookeeper.id
            assigns(:current_zookeeper).should == old_zookeeper
            session[:current_zookeeper_id].should == old_zookeeper.id
          end
        end
        describe "with an invalid pre-existing current_zookeeper" do
          it "sets current_zookeeper to the first zookeeper, and resets the session" do
            Zookeeper.should_receive(:find_by_id).with(1).and_return(@zookeeper)
            get :show_current, nil, :current_zookeeper_id => 1
            assigns(:current_zookeeper).should == @zookeeper
            session[:current_zookeeper_id].should == @zookeeper.id
          end
        end
      end
      describe "with no persisted zookeepers" do
        describe "with no previous zookeeper" do
          it "redirects to the root path, with no current_zookeeper_id in session" do
            Zookeeper.should_receive(:find_by_id).with(1).and_return(nil)
            Zookeeper.stub_chain(:order, :first).and_return nil
            get :show_current, nil, :current_zookeeper_id => 1
            session[:current_zookeeper_id].should be nil
            response.should redirect_to root_path
          end
        end
        describe "with a previous current zookeeper" do
          it "redirects to the root path, with no current_zookeeper_id in session" do
            Zookeeper.should_receive(:find_by_id).with(1).and_return(nil)
            Zookeeper.stub_chain(:order, :first).and_return nil
            get :show_current, nil, :current_zookeeper_id => 1
            session[:current_zookeeper_id].should be nil
            response.should redirect_to root_path
          end
        end
      end
#/