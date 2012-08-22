class ApplicationController < ActionController::Base
  protect_from_forgery

  require 'thrift/blur'
  require 'blur_thrift_client'

  before_filter :show_zookeeper_options
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
    @current_user ||= current_user_session && current_user_session.user
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
    @current_zookeeper ||= Zookeeper.find_by_id(params[:zookeeper_id] || session[:current_zookeeper_id])
    if @current_zookeeper.nil?
      determine_error
    else
      set_zookeeper @current_zookeeper.id if params[:zookeeper_id]
    end
  end

  def set_zookeeper_with_preference
    user_zk_pref = current_user.zookeeper_preference
    if user_zk_pref.name.to_i > 0
      if Zookeeper.find_by_id(user_zk_pref.value).nil?
        flash[:error] = "Your preferred Zookeeper no longer exists, your preference has been reset!"
        user_zk_pref.name = 0
        user_zk_pref.save
      else
        session[:current_zookeeper_id] = user_zk_pref.value
      end
    end
  end

  def set_show_zookeeper
    set_zookeeper params[:id]
  end

  def set_zookeeper(id)
    id = id.to_i
    if id.nil? || Zookeeper.find_by_id(id).nil?
      determine_error
    else
      session[:current_zookeeper_id] = id
    end
  end

  def determine_error
    if request.xhr?
      render :status => :conflict, :text => "No Current Zookeeper"
    else
      redirect_to root_path
    end
  end

  def zookeepers
    @zookeepers ||= Zookeeper.order 'name'
  end

  def show_zookeeper_options
    zookeepers if @current_zookeeper.nil?
  end

  def current_user_session
    @current_user_session ||= UserSession.find
  end
end
