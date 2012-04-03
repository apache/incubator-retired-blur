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
    @current_zookeeper = Zookeeper.find_by_id(params[:zookeeper_id])
    if @current_zookeeper.nil?
      if request.xhr?
        render :status => :conflict, :text => "No Current Zookeeper"
      else
        flash[:error] = "A Zookeeper with that particular id does not exist!"
        redirect_to root_path
      end
    else
      session[:current_zookeeper_id] = @current_zookeeper.id
    end
  end

  def set_zookeeper
    if Zookeeper.find_by_id(params[:id]).nil?
      flash[:error] = "A Zookeeper with that particular id does not exist!"
      redirect_to root_path
    else
      session[:current_zookeeper_id] = params[:id]
    end
  end

  def zookeepers
    @zookeepers ||= Zookeeper.order 'name'
  end

  def current_user_session
    @current_user_session ||= UserSession.find
  end
end