# Base Application Controller
class ApplicationController < ActionController::Base
  protect_from_forgery

  require 'thrift/blur'
  require 'blur_thrift_client'

  before_filter :current_user_session, :current_user
  helper_method :license, :current_user

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
    session = current_user_session
    @current_user ||= session && session.user
  end

  def help
    tab = params['tab']
    respond_to do |format|
      format.html {render :partial => 'layouts/help_menu', :locals => {:tab => tab}}
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

      redirect_to root_path and return unless @current_zookeeper

      @current_zookeeper
    end

    def zookeepers
      @zookeepers ||= Zookeeper.order 'name'
    end

    def current_user_session
      @current_user_session ||= UserSession.find
    end
end
