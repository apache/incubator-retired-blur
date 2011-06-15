class ApplicationController < ActionController::Base
  protect_from_forgery

	require 'thrift/blur'
	require 'blur_thrift_client'

  enable_authorization do |exception|
    puts exception
    if current_user
      redirect_to root_url, :alert => "Unauthorized"
    else
      redirect_to login_path, :alert => "Please login"
    end
  end

  before_filter :current_user_session, :current_user
  
  def thrift_client
    BlurThriftClient.client
  end

  private
    
    def current_user_session
      return @current_user_session if defined? @current_user_session
      @current_user_session = UserSession.find
    end

    def current_user
      return @current_user if defined? @current_user
      @current_user = current_user_session && current_user_session.user
    end
end
