require 'spec_helper'

describe UsersController do
  fixtures :all

  describe "GET index" do
    let(:users) { [] }

    before do
      User.stub(:all).and_return(users)
    end

    it "should get and assign users" do
      User.should_receive(:all)
      get :index
      assigns(:users).should == users
    end
    
    it "should render index template" do
      get :index
      response.should render_template(:index)
    end
  end

  describe "GET show" do
    let(:user) { mock_model(User).as_null_object }

    before do
      User.stub(:find).and_return(user)
    end

    it "should find and assign user" do
      User.should_receive(:find).with('id').and_return(user)
      get :show, :id => 'id'
      assigns(:user).should == user
    end

    it "should render show template" do
      get :show, :id => 'id'
      response.should render_template(:show)
    end
  end

  describe "GET new" do
    let(:user) { mock_model(User).as_null_object }

    it "should create a new user" do
      User.should_receive(:new).and_return(user)
      get :new
    end

    it "should render new layout" do
      get :new
      response.should render_template(:new)
    end
  end


  describe "POST create" do
    let(:user) { mock_model(User).as_null_object }

    before do
      User.stub(:new).and_return(user)
      @valid_user = {
        'username' => 'bob',
        'email' => 'bob@example.com',
        'password' => 'password',
        'password_confirmation' => 'password'
      }
    end

    it "creates a new user" do
      User.should_receive(:new).with(@valid_user).and_return(user)
      post :create, :user => @valid_user
    end

    context "when the message saves successfully" do
      it "redirects to the root path" do
        post :create
        response.should redirect_to(root_path)
      end
    end

    context "when the message saves unsuccessfully" do
      it "renders the new template" do
        user.stub(:save).and_return(false)
        post :create
        response.should render_template(:new)
      end
    end
  end

  describe "GET edit" do

    let(:user) { mock_model(User).as_null_object }

    before do
      User.stub(:find).and_return(user)
    end

    it "should find and assign the user" do
      User.should_receive(:find).with('id').and_return(user)
      get :edit, :id => 'id'
      assigns(:user).should == user
    end

    it "should render the edit template" do
      get :edit, :id => 'id'
      response.should render_template(:edit)
    end
  end

  describe "PUT update" do

    let(:user) { mock_model(User) }

    before do
      User.stub(:find).and_return(user)
    end

    it "should find and assign the user" do
      User.should_receive(:find).with('id').and_return(user)
      user.should_receive(:update_attributes).and_return(true)
      put :update, :id => 'id'
      assigns(:user).should == user
    end

    context "update is successful" do

      it "should redirect to the user and include notice" do
        user.should_receive(:update_attributes).with('username' => 'newname').and_return(true)
        put :update, {:id => 'id', :user => {:username => 'newname'}}
        response.should redirect_to(user)
        flash[:notice].should_not be_blank
      end

    end

    context "update is unsuccessful" do

      it "should render the edit template" do
        user.should_receive(:update_attributes).with("username" => "newname").and_return(false)
        put :update, {:id => 'id', :user => {'username' => 'newname'}}
        response.should render_template(:edit)
      end
    end
  end

  
  describe "DELETE destroy" do
    it "should find and destroy the user" do
      user = mock_model(User).as_null_object
      User.stub(:find).with('id').and_return(user)
      user.should_receive(:destroy)
      delete :destroy, :id => 'id' 
    end
  end
end

