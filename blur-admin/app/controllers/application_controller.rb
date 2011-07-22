class ApplicationController < ActionController::Base
  protect_from_forgery

	require 'thrift/blur'
	require 'blur_thrift_client'

  before_filter :current_user_session, :current_user

  enable_authorization do |exception|
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
    @current_user ||= current_user_session && current_user_session.user
  end

  private

    def current_zookeeper
      # Load zookeeper from session. if that doesn't work, then delete id in session
      @current_zookeeper = Zookeeper.find_by_id(session[:current_zookeeper_id]) or session.delete :current_zookeeper_id

      #if that doesn't work, get first.  If that works, then set id in session
      @current_zookeeper ||= Zookeeper.first and session[:current_zookeeper_id] = @current_zookeeper.id

      # If there are no zookeepers, redirect to the dashboard
      unless @current_zookeeper
        redirect_to root_path, :alert => "No Zookeeper Instances" and return
      end

      # else return current zookeeper
      @current_zookeeper
    end

    def zookeepers
      @zookeepers ||= Zookeeper.all
    end

    def current_user_session
      @current_user_session ||= UserSession.find
    end
end
