require "spec_helper"
require "cancan/matchers"

describe Ability do
  describe "when not logged in" do
    before(:each) do
      @ability = Ability.new(nil)
    end

    it "can create a user (register) with username, email, password" do
      @ability.should be_able_to :new, :users
      @ability.should be_able_to :create, :users, :username
      @ability.should be_able_to :create, :users, :email
      @ability.should be_able_to :create, :users, :password
      @ability.should be_able_to :create, :users, :password_confirmation
    end

    it "can not create a user (register) with roles" do
      @ability.should_not be_able_to :create, :users, :admin
      @ability.should_not be_able_to :create, :users, :editor
    end

    it "can create a session (log in)" do
      @ability.should be_able_to :create, :user_sessions 
      @ability.should be_able_to :new, :user_sessions
    end

    it "can not view pages" do
      @ability.should_not be_able_to :access, :blur_tables
      @ability.should_not be_able_to :access, :env
      @ability.should_not be_able_to :access, :search
      @ability.should_not be_able_to :access, :blur_queries
    end
  end

  describe "when a user with no roles" do
    before(:each) do
      @user = User.new
      @user.stub(:id).and_return(123)
      @user.stub(:has_role?).with(:editor).and_return(false)
      @user.stub(:has_role?).with(:admin).and_return(false)
      @user.stub(:has_role?).with(:auditor).and_return(false)
      @user.stub(:has_role?).with(:reader).and_return(false)
      @ability = Ability.new @user
    end

    it "can view pages" do
      @ability.should be_able_to :index, :blur_tables
      @ability.should be_able_to :hosts, :blur_tables
      @ability.should be_able_to :show, :env
      @ability.should be_able_to :show, :search
      @ability.should be_able_to :index, :blur_queries
      @ability.should be_able_to :more_info, :blur_queries
    end

    it "can view, edit and delete itself" do
      @ability.should be_able_to :show, @user
      @ability.should be_able_to :edit, @user
      @ability.should be_able_to :destroy, @user
    end

    it "can not view, edit, update, or delete other users" do
      other_user = User.new
      @ability.should_not be_able_to :show, other_user
      @ability.should_not be_able_to :edit, other_user
      @ability.should_not be_able_to :update, other_user
      @ability.should_not be_able_to :destroy, other_user
    end

    it "can not view the admin page" do
      @ability.should_not be_able_to :index, :users
    end

    it "can perform queries" do
      @ability.should be_able_to :filters, :search
      @ability.should be_able_to :create, :search
    end

    it "can not create a new user or session" do
      @ability.should_not be_able_to :new, :user_sessions
      @ability.should_not be_able_to :create, :user_sessions
      @ability.should_not be_able_to :new, :users
      @ability.should_not be_able_to :create, :users
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

    it "can not update own roles" do
      @ability.should_not be_able_to :update, @user, :admin
      @ability.should_not be_able_to :update, @user, :editor 
    end

    it "can not audit blur_queries" do
      @ability.should_not be_able_to :audit, :blur_queries
    end
  end

  describe "when an editor" do
    before(:each) do
      @user = User.new
      @user.stub(:id).and_return(123)
      @user.stub(:has_role?).with(:editor).and_return(true)
      @user.stub(:has_role?).with(:admin).and_return(false)
      @user.stub(:has_role?).with(:auditor).and_return(false)
      @user.stub(:has_role?).with(:reader).and_return(false)
      @ability = Ability.new @user
    end
  
    it "can enable, disable, and delete tables" do
      @ability.should be_able_to :update, :blur_tables
      @ability.should be_able_to :destroy, :blur_tables
    end

    it "can cancel queries" do
      @ability.should be_able_to :update, :blur_queries
    end
  end

  describe "when an auditor" do
    before(:each) do
      @user = User.new
      @user.stub(:id).and_return(123)
      @user.stub(:has_role?).with(:editor).and_return(false)
      @user.stub(:has_role?).with(:admin).and_return(false)
      @user.stub(:has_role?).with(:auditor).and_return(true)
      @user.stub(:has_role?).with(:reader).and_return(false)
      @ability = Ability.new @user
    end
  
    it "can audit blur_queries" do
      @ability.should be_able_to :audit, :blur_queries
    end
  end
  
  describe "when an admin" do
    before(:each) do
      @user = User.new
      @user.stub(:id).and_return(123)
      @user.stub(:has_role?).with(:editor).and_return(false)
      @user.stub(:has_role?).with(:admin).and_return(true)
      @user.stub(:has_role?).with(:auditor).and_return(false)
      @user.stub(:has_role?).with(:reader).and_return(false)
      @ability = Ability.new @user
      @other_user = User.new
    end
    it "can view, edit, and delete other users" do
      @ability.should be_able_to :index, :users
      @ability.should be_able_to :show, @other_user
      @ability.should be_able_to :edit, @other_user
      @ability.should be_able_to :destroy, @other_user
    end

    it "can update other users' roles" do
      @ability.should be_able_to :update, @other_user, :admin
      @ability.should be_able_to :update, @other_user, :editor
    end

    it "can not update other users' username, email, or password" do
      @ability.should_not be_able_to :update, @other_user, :username
      @ability.should_not be_able_to :update, @other_user, :email
      @ability.should_not be_able_to :update, @other_user, :password
      @ability.should_not be_able_to :update, @other_user, :password_confirmation
    end

    it "can create users with roles" do
      @ability.should be_able_to :create, :users, :admin
      @ability.should be_able_to :create, :users, :editor
    end
  end
end
