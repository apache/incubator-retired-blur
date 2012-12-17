require 'spec_helper'

describe ApplicationController do
  describe "user actions" do
    before do
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

    describe "zookeeper preference" do
      it "should grab the current users preference" do
        controller.should_receive(:current_user).and_return @user
        controller.send(:set_zookeeper_with_preference)
      end

      it "should do nothing if the default pref is chosen" do
        zookeeper_pref = FactoryGirl.create(:zookeeper_pref, :name => 0)
        @user.stub!(:zookeeper_preference).and_return(zookeeper_pref)
        Zookeeper.should_not_receive(:find_by_id)
        controller.send(:set_zookeeper_with_preference)
      end

      it "should set the zookeeper if the pref isnt the default and it exists" do
        controller.should_receive(:set_zookeeper).with @user.zookeeper_preference.value
        Zookeeper.stub!(:find_by_id).and_return true
        controller.send(:set_zookeeper_with_preference)
      end

      it "should error and reset your preference when the zookeeper no longer exists" do
        zookeeper_pref = FactoryGirl.create(:zookeeper_pref)
        @user.stub!(:zookeeper_preference).and_return(zookeeper_pref)
        Zookeeper.stub!(:find_by_id).and_return nil
        zookeeper_pref.should_receive(:name=)
        zookeeper_pref.should_receive(:save)
        controller.send(:set_zookeeper_with_preference)
        controller.flash[:error].should_not be_empty
      end
    end
  end

  describe "Current Zookeeper" do 
    before do 
      @zookeeper = FactoryGirl.create :zookeeper
      Zookeeper.stub!(:find_by_id).and_return @zookeeper
    end

    it "should set the zookeeper to the zookeeper id when it is given" do 
      Zookeeper.should_receive(:find_by_id).with(@zookeeper.id)
      controller.params[:zookeeper_id] = @zookeeper.id
      controller.should_receive(:set_zookeeper).with @zookeeper.id
      zookeeper = controller.current_zookeeper
      assigns(:current_zookeeper).should == @zookeeper
      zookeeper.should == @zookeeper
    end

    it "should set the zookeeper to the zookeeper id in the session when there isnt an id" do 
      Zookeeper.should_receive(:find_by_id).with(@zookeeper.id)
      controller.session[:current_zookeeper_id] = @zookeeper.id
      zookeeper = controller.current_zookeeper
      assigns(:current_zookeeper).should == @zookeeper
      zookeeper.should == @zookeeper
    end

    it "should redirect to the root page when it doesnt find a zookeeper" do 
      Zookeeper.stub!(:find_by_id).and_return nil
      controller.stub! :redirect_to
      controller.should_receive(:redirect_to).with '/'
      controller.current_zookeeper
    end

    it "should render a conflict when the request is xhr and it doesnt find a zookeeper" do 
      Zookeeper.stub!(:find_by_id).and_return nil
      controller.request.stub!(:xhr?).and_return true
      controller.stub! :render
      controller.should_receive(:render).with({:status=>:conflict, :text=>"No Current Zookeeper"})
      controller.current_zookeeper
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

  describe "private methods" do 
    it "set_zookeeper should set the zookeeper if the id is new" do 
      controller.session[:current_zookeeper_id] = 2
      controller.session.should_receive :[]=
      controller.send(:set_zookeeper, '1')
    end

    it "set_zookeeper should not set the zookeeper if the id is new" do 
      controller.session[:current_zookeeper_id] = 1
      controller.session.should_not_receive :[]=
      controller.send(:set_zookeeper, '1')
    end
  end

  describe "lock down api" do 
    describe "format" do
      it "should be set to html if the format is blank" do 
        respond = {:html => {}}
        respond.should_receive(:[]).with(:html).and_return({})
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        controller.send(:lock_down_api)
      end

      it "should be set to the given format when it is specified" do 
        controller.params[:format] = 'json'
        respond = {:json => {}}
        respond.should_receive(:[]).with(:json).and_return({})
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        controller.send(:lock_down_api)
      end

      it "should raise an error when there are no accepted actions" do 
        respond = {:html => nil}
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        expect {
          controller.send(:lock_down_api)
        }.to raise_error "Unaccepted Format for this Action!"
      end
    end

    describe "only block" do
      it "should not raise an error when the current action is present" do
        controller.params[:action] = :index
        respond = {:html => {}}
        respond.stub!(:[]).and_return({:only => [:index]})
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        expect {
          controller.send(:lock_down_api)
        }.to_not raise_error
      end

      it "should raise an error when the current action is in the not present" do
        controller.params[:action] = :index
        respond = {:html => {}}
        respond.stub!(:[]).and_return({:only => [:show]})
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        expect {
          controller.send(:lock_down_api)
        }.to raise_error "Unaccepted Format for this Action!"
      end
    end

    describe "except block" do
      it "should raise an error when the current action is present" do
        controller.params[:action] = :index
        respond = {:html => {}}
        respond.stub!(:[]).and_return({:except => [:index]})
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        expect {
          controller.send(:lock_down_api)
        }.to raise_error
      end

      it "should raise an error when the current action is in the not present" do
        controller.params[:action] = :index
        respond = {:html => {}}
        respond.stub!(:[]).and_return({:except => [:show]})
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        expect {
          controller.send(:lock_down_api)
        }.to_not raise_error "Unaccepted Format for this Action!"
      end
    end

    it "should not error when the action is present" do
      controller.params[:action] = :index
        respond = {:html => {}}
        controller.stub!(:mimes_for_respond_to).and_return(respond)
        expect {
          controller.send(:lock_down_api)
        }.to_not raise_error "Unaccepted Format for this Action!"
    end
  end
end