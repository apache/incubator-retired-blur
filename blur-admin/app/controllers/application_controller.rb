class ApplicationController < ActionController::Base
  protect_from_forgery

	require 'thrift/blur'
	require 'blur_thrift_client'

  enable_authorization do |exception|
    puts exception
    if current_user
      if can? :show, :zookeepers
        redirect_to root_url, :alert => "Unauthorized"
      else
        redirect_to logout_url, :alert => "Unauthorized"
      end
    else
      redirect_to login_path, :alert => "Please login"
    end
  end

  before_filter :current_user_session, :current_user 

  private

    def current_user_session
      @current_user_session ||= UserSession.find
    end

    def current_user
      @current_user ||= current_user_session && current_user_session.user
    end

    def current_zookeeper
      #Reset current zookeeper instance if previous zookeeper no longer exists
      puts session[:current_blur_zookeper_instance_id]
      if session[:current_zookeeper_id] and !Zookeeper.find_by_id session[:current_zookeeper_id]
        session.delete :current_zookeeper_id
        redirect_to zookeeper_path, :notice => "Your previous blur zookeeper instance no longer exists"
      end

      #If no current instance in session, then default to first record
      session[:current_zookeeper_id] ||= Zookeeper.first.id
      @current_zookeeper = Zookeeper.find session[:current_zookeeper_id]
    end
end
