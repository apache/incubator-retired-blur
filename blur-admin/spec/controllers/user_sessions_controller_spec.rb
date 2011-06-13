require 'spec_helper'

describe UserSessionsController do

  def mock_user_session(stubs={})
    @mock_user_session ||= mock(UserSession, stubs).as_null_object
  end

  before(:each) do
    activate_authlogic
    @mock_user_session = nil
  end

  describe "GET 'new'" do
    it "assigns a new user_session as @user_session" do
      UserSession.should_receive(:new).at_least(1).times.and_return(mock_user_session)
      get :new
      assigns(:user_session).should be(mock_user_session)
    end
    
    it "should render new view" do
      get :new
      response.should render_template :new
    end
  end

  describe "POST create" do
    it "assigns a new session to @user_session and attempts to save" do
      UserSession.stub(:new) {mock_user_session}
      mock_user_session.should_receveive(:save)
      post :create
      assigns(:user_session).should be(mock_user_session)
    end

    context "with valid user parameters" do
      it "redirects to root_url and sends a notice" do
        UserSession.stub(:new) {mock_user_session(:save => true)}
        post :create
        response.should redirect_to(root_url)
      end
    end
    context "with invalid user parameters" do
      it "renders the new action" do
        UserSession.stub(:new) {mock_user_session(:save => false)}
        post :create
        response.should render_template(:new)
      end
    end
  end

  describe "DELETE 'destroy'" do
    it "finds and destroys current user session" do
      UserSession.should_receive(:find).at_least(1).times.and_return(mock_user_session)
      mock_user_session.should_receive(:destroy)
      delete :destroy
    end

    it "redirects to root_url with notice" do
      UserSession.stub(:find).and_return(mock_user_session)
      delete :destroy
      response.should redirect_to(login_path)
      flash[:notice].should_not be_blank


    end
  end
end
