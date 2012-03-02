class ApplicationController < ActionController::Base
  protect_from_forgery
  check_authorization
  
  require 'thrift/blur'
  require 'blur_thrift_client'

  before_filter :current_user_session, :current_user
  helper_method :license, :current_user

  rescue_from CanCan::AccessDenied do |exception|
    if current_user
      if can? :index, :zookeepers
        redirect_to root_url, :alert => "Unauthorized"
      else
        redirect_to logout_url, :alert => "Unauthorized"
      end
    else
      redirect_to login_path, :alert => "Please login"
    end
  end

  def current_user
    session = current_user_session
    @current_user ||= session && session.user
  end

  def help
    @tab = params['tab']
    respond_to do |format|
      format.html {render :partial => 'layouts/help_menu', :locals => {:tab => @tab}}
    end
  end

  def license
    @license ||= License.first
  end

  private
  def current_zookeeper
    if @current_zookeeper.nil? || @current_zookeeper.id != session[:current_zookeeper_id]
      @current_zookeeper = Zookeeper.find_by_id(session[:current_zookeeper_id])
      session.delete :current_zookeeper_id if @current_zookeeper.nil?
      session[:current_zookeeper_id] = @current_zookeeper.id unless @current_zookeeper.nil?
    end
    if @current_zookeeper.nil?
      if request.xhr?
        render :status => :conflict, :text => "No Current Zookeeper"
      else
        redirect_to root_path
      end
    end
    @current_zookeeper
  end

  def zookeepers
    @zookeepers ||= Zookeeper.order 'name'
  end

  def current_user_session
    @current_user_session ||= UserSession.find
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