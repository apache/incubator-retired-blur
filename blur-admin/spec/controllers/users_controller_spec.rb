require 'spec_helper'

describe UsersController do
  describe "actions" do
    before(:each) do
      #Universal Setup
      setup_tests

      # Add preferences to the current user
      @user = FactoryGirl.create :user_with_preferences # user with all roles
      User.stub(:find).and_return @user
    end

    describe "GET index" do
      before(:each) do
        @users = []
        User.stub(:all).and_return(@users)
      end

      it "should get and assign users" do
        User.should_receive(:all)
        get :index
        assigns(:users).should == @users
      end

      it "should render index template" do
        get :index
        response.should render_template(:index)
      end
    end

    describe "GET show" do
      before(:each) do
        @table = FactoryGirl.create_list :blur_table, 3
        @user.stub(:saved_cols)
        BlurTable.stub(:all).and_return(@table)
      end

      it "should find and assign user" do
        User.should_receive(:find).with('id').and_return(@user)
        get :show, :id => 'id'
        assigns(:user).should == @user
      end

      it "should find and assign preferences" do
        @user.should_receive(:column_preference).at_least(1).times
        @user.should_receive(:zookeeper_preference).at_least(1).times
        get :show, :id => 'id'
        assigns(:column_preference).should == @user.column_preference
        assigns(:zookeeper_preference).should == @user.zookeeper_preference
      end

      it "should find and create a list of all the table choices" do
        get :show, :id => 'id'
        assigns(:choices).should == ['ColumnFamily1', 'ColumnFamily2', 'ColumnFamily3']
      end

      it "should render show template" do
        get :show, :id => 'id'
        response.should render_template(:show)
      end
    end

    describe "GET new" do
      let(:user) { mock_model(User).as_null_object }

      it "should create a new user" do
        User.should_receive(:new).at_least(1).times.and_return(user)
        get :new
      end

      it "should render new layout" do
        get :new
        response.should render_template(:new)
      end
    end


    describe "POST create" do
      before(:each) do
        User.stub(:new).and_return(@user)
        @valid_user = {
          'username' => 'bob',
          'email' => 'bob@example.com',
          'password' => 'password',
          'password_confirmation' => 'password'
        }
      end

      it "creates a new user" do
        User.should_receive(:new).with(@valid_user).and_return(@user)
        post :create, :user => @valid_user
      end

      context "when the message saves successfully" do
        it "redirects to the users path when it can? index users" do
          @user.stub!(:save).and_return(true)
          post :create
          response.should redirect_to(users_path)
        end

        it "redirects to the user's page when it cannot? index users" do
          @user.stub!(:save).and_return(true)
          controller.stub!(:can?).and_return(false)
          post :create
          response.should redirect_to(@user)
        end
      end

      context "when the message saves unsuccessfully" do
        it "renders the new template" do
          @user.stub!(:save).and_return(false)
          post :create
          response.should render_template(:new)
        end
      end
    end

    describe "GET edit" do
      it "should find and assign the user" do
        User.should_receive(:find).with('id').and_return(@user)
        get :edit, :id => 'id'
        assigns(:user).should == @user
      end

      it "should render the edit template" do
        get :edit, :id => 'id'
        response.should render_template(:edit)
      end
    end

    describe "PUT update" do
      it "should find and assign the user" do
        User.should_receive(:find).with(@user.id.to_s)
        put :update, :id => @user.id, :user => {:name => 'Bob'}
        assigns(:user).should == @user
      end

      context "When updating the attributes succeeds" do
        before(:each) do
          @user.stub!(:update_attributes).and_return true
        end

        it "should redirect to the admin page and include notice when admin updates a user" do
          controller.stub!(:can?).and_return(false)
          put :update, :id => @user.id, :user => {:name => 'Bob'}
          response.should redirect_to(@user)
          flash[:notice].should_not be_blank
        end

        it "should redirect to the user and include notice when user updates himself" do
          controller.stub!(:can?).and_return(true)
          put :update, :id => @user.id, :user => {:name => 'Bob'}
          response.should redirect_to users_path
          flash[:notice].should_not be_blank
        end
      end

      context "When updating the attributes fails" do
        it "should render the edit template when update fails" do
          @user.stub!(:update_attributes).and_return(false)
          put :update, :id => @user.id, :user => {:name => 'Bob'}
          response.should render_template(:edit)
        end
      end

      it "should log an audit event" do
        Audit.should_receive :log_event
        put :update, :id => @user.id, :user => {:name => 'Bob'}
      end
    end

    describe "DELETE destroy" do
      it "should find and destroy the user" do
        @user.should_receive(:destroy)
        delete :destroy, :id => @user.id
      end

      it "should redirect to the users_path" do
        delete :destroy, :id => @user.id
        response.should redirect_to(users_path)
      end

      it "should log an audit event" do
        Audit.should_receive :log_event
        delete :destroy, :id => @user.id
      end

    end
  end
end
