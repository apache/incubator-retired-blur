class ApplicationController < ActionController::Base
  protect_from_forgery

	require 'thrift/blur'
	require 'blur_thrift_client'

  enable_authorization do |exception|
    puts exception
    if current_user
      if can? :show, :blur_zookeeper_instances
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

    def current_blur_zookeeper_instance

    end
end
