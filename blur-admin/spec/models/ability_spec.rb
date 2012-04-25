require "spec_helper"

def controller_actions
  # this method returns a hash of controllers each with an array of their respective
  # actions.  Use this list to make sure unauthorized users are not able to access any
  # actions in a controller.  Note that the action_methods also includes some callbacks,
  # so this list cannot be used for determining that a user *can* do all of the actions
  # in a controller (use cancan 2.0's 'access' for that).
  
  Dir[File.join Rails.root, 'app', 'controllers', '*'].each {|file| load file}
  @controller_actions ||= ApplicationController.subclasses.reduce({}) do |memo, controller|
    memo[controller.to_s.gsub("Controller", "").underscore.to_sym] = 
      controller.action_methods.collect {|method| method.to_sym}
    memo
  end
end

describe Ability do
  describe "when not logged in" do
    before(:each) do
      @ability = Ability.new(nil)
    end

    it "can create a user (register) with username, email, password" do
      @ability.should be_able_to :new,    :users
      @ability.should be_able_to :create, :users, :username
      @ability.should be_able_to :create, :users, :email
      @ability.should be_able_to :create, :users, :password
      @ability.should be_able_to :create, :users, :password_confirmation
    end

    it "can not create a user (register) with roles" do
      User::ROLES.each do |role|
        @ability.should_not be_able_to :create, :users, role
      end
    end

    it "can create a session (log in)" do
      @ability.should be_able_to :create, :user_sessions 
      @ability.should be_able_to :new, :user_sessions
    end

    it "can not access pages" do
      # actions are automatically added to this check.  Thus, you have to specifically
      # filter actions that should be available to non logged-in users

      @actions = controller_actions

      #filter out actions available to non logged in users
      @actions[:user_sessions] -= [:create, :new]
      @actions[:users]         -= [:new, :create]

      @actions.each do |controller, actions|
        actions.each {|action| @ability.should_not be_able_to action, controller}
      end

    end
  end

  describe "when a user with no roles" do
    before(:each) do
      @user = FactoryGirl.create :user, :roles => []
      @ability = Ability.new @user
    end

    it "can view, edit and delete itself" do
      @ability.should be_able_to :show, @user
      @ability.should be_able_to :edit, @user
      @ability.should be_able_to :destroy, @user
    end

    it "can log out" do
      @ability.should be_able_to :destroy, :user_sessions
    end

    it "can update own username, email, and password" do
      @ability.should be_able_to :update, @user, :username
      @ability.should be_able_to :update, @user, :email
      @ability.should be_able_to :update, @user, :password
      @ability.should be_able_to :update, @user, :password_confirmation
   end

    it "can not view, edit, update, or delete other users" do
      other_user = User.new
      @ability.should_not be_able_to :show, other_user
      @ability.should_not be_able_to :edit, other_user
      @ability.should_not be_able_to :update, other_user
      @ability.should_not be_able_to :destroy, other_user
    end

    it "can not update own roles" do
      @ability.should_not be_able_to :update, @user, :admin
      @ability.should_not be_able_to :update, @user, :editor 
    end

    it "can not view query_strings on blur_query page" do
      @ability.should_not be_able_to :index, :blur_queries, :query_string
    end

    it "can not access pages" do
      # actions are automatically added to this check.  Thus, you have to specifically
      # filter actions that should be available to non logged-in users.
      # NOTE:  These are just the vanilla actions, so if the action depends on what
      # attribute is being updated, or what value an attribute is (i.e. checking to 
      # make sure updated object is the user's) it will pass, so those cases must be
      # tested seperately.

      @actions = controller_actions

      #filter out actions available to non logged in users
      @actions[:users] -= [:show, :edit, :destroy, :update]
      @actions[:user_sessions] -= [:destroy]

      @actions.each do |controller, actions|
        actions.each {|action| @ability.should_not be_able_to action, controller}
      end

    end

  end

  describe "when a reader" do
    before(:each) do
      @user = FactoryGirl.create :user, :roles => ['reader']
      @ability = Ability.new @user
    end

    it "can view pages" do
      @ability.should be_able_to :index, :blur_tables
      @ability.should be_able_to :index, :zookeepers
      @ability.should be_able_to :index, :blur_queries
      @ability.should be_able_to :more_info, :blur_queries
    end

    it "can not view query strings" do
      @ability.should_not be_able_to :more_info, :blur_queries, :query_string
      @ability.should_not be_able_to :index, :blur_queries, :query_string
    end

    it "can not change own column preferences" do
      @preference = FactoryGirl.create :preference, :user_id => @user.id, :pref_type => 'column'
      @ability.should_not be_able_to :update, @preference
    end
  end

  describe "when an editor" do
    before(:each) do
      @user = FactoryGirl.create :user, :roles => ['editor']
      @ability = Ability.new @user
    end
  
    it "can enable, disable, and delete tables" do
      @ability.should be_able_to :enable, :blur_tables
      @ability.should be_able_to :disable, :blur_tables
      @ability.should be_able_to :destroy, :blur_tables
    end

    it "can cancel queries" do
      @ability.should be_able_to :update, :blur_queries
    end
  end

  describe "when an auditor" do
    before(:each) do
      @user = FactoryGirl.create :user, :roles => ['auditor']
      @ability = Ability.new @user
    end
  
    it "can view blur query string" do
      @ability.should be_able_to :index, :blur_queries, :query_string
      @ability.should be_able_to :more_info, :blur_queries, :query_string
    end
  end
  
  describe "when an admin" do
    before(:each) do
      @user = FactoryGirl.create :user, :roles => ['admin']
      @ability = Ability.new @user
      @other_user = User.new
    end

    it "can edit, and delete other users" do
      @ability.should be_able_to :index, :users
      @ability.should be_able_to :edit, @other_user
      @ability.should be_able_to :destroy, @other_user
    end

    it "can update other users' roles" do
      @ability.should be_able_to :update, @other_user, :roles
    end

    it "can not view other individual users" do
      @ability.should_not be_able_to :show, @other_user
    end

    it "can not update other users' username, or password" do
      @ability.should_not be_able_to :update, @other_user, :username
      @ability.should_not be_able_to :update, @other_user, :password
      @ability.should_not be_able_to :update, @other_user, :password_confirmation
    end
    
    it "can update other users' email" do
      @ability.should be_able_to :update, @other_user, :email
    end

    it "can create users with roles" do
      @ability.should be_able_to :create, :users, :admin
      @ability.should be_able_to :create, :users, :editor
    end
  end

  describe "when a searcher" do
    before do
      @user = FactoryGirl.create :user, :roles => ['searcher']
      @ability = Ability.new @user
    end

    it "can view and use the search page" do
      @ability.should be_able_to :access, :searches
    end
    it "can change own column preferences" do
      @preference = FactoryGirl.create :preference, :user_id => @user.id, :pref_type => 'column'
      @ability.should be_able_to :update, @preference
    end
    it "can not change own filter preferences" do
      @preference = FactoryGirl.create :preference, :user_id => @user.id, :pref_type => 'filter'
      @ability.should_not be_able_to :update, @preference
    end
  end
end
