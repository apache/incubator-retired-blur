require 'spec_helper'

describe UserSessionsController do
  describe "actions" do
    before(:each) do
      # setup the tests
      activate_authlogic
      setup_tests

      # usersessions specific
      @user_session = UserSession.new({:username => @user.username, :password => @user.password, :commit => "Log In"})
      @user_session.stub!(:user).and_return @user
      controller.stub!(:current_user_session).and_return @user_session
    end

    describe "GET 'new'" do
      it "assigns a new user_session as @user_session" do
        UserSession.should_receive(:new).at_least(1).times.and_return(@user_session)
        get :new
        assigns(:user_session).should == (@user_session)
      end

      it "should render new view" do
        get :new
        response.should render_template :new
      end
    end

    describe "POST create" do
      it "assigns a new session to @user_session and saves successfully" do
        UserSession.stub(:new).and_return @user_session
        @user_session.should_receive(:save).and_return true
        post :create, :user_session => {:username => @user.username, :password => @user.password, :commit => "Log In"}
        assigns(:user_session).should be(@user_session)
        response.should redirect_to(root_path)
      end

      it "assigns a new session to @user_session and saves unsuccessfully" do
        UserSession.stub(:new).and_return @user_session
        @user_session.should_receive(:save).and_return false
        post :create, :user_session => {:username => @user.username, :password => @user.password, :commit => "Log In"}
        response.should render_template(:new)
      end
    end

    describe "DELETE 'destroy'" do
      it "finds and destroys current user session" do
        @user_session.should_receive(:destroy)
        delete :destroy
      end

      it "redirects to root_url with notice" do
        delete :destroy
        response.should redirect_to(login_path)
        flash[:notice].should_not be_blank
      end
    end
  end
end
