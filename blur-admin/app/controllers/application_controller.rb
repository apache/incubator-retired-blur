class ApplicationController < ActionController::Base
  protect_from_forgery

  before_filter :lock_down_api

  respond_to :html, :only => :help

  require 'thrift/blur'
  require 'blur_thrift_client'

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
      format.any  { render :partial => 'layouts/help_menu' }
    end
  end

  def license
    @license ||= License.first
  end

  # Populates the @current_zookeeper instance variable
  def current_zookeeper
    # Find the zookeeper with the given or the stored session id
    @current_zookeeper ||= Zookeeper.find_by_id(params[:zookeeper_id] || session[:current_zookeeper_id])
    if @current_zookeeper.nil?
      zookeeper_error
    else
      # Set the zookeeper
      set_zookeeper @current_zookeeper.id
    end
    @current_zookeeper
  end
  #Catches application errors and redirects to the custom error pages
  unless Rails.application.config.consider_all_requests_local
    rescue_from Exception, :with => :error_500
    rescue_from ActionController::RoutingError, :with => :error_404
    rescue_from ActionController::UnknownController, :with => :error_404
    rescue_from AbstractController::ActionNotFound, :with => :error_404
    rescue_from ActiveRecord::RecordNotFound, :with => :error_404
    rescue_from ActionController::InvalidAuthenticityToken, :with => :error_422
  end

  def set_zookeeper(id)
    # Convert all inputs to an int
    id = id.to_i
    # Avoids a DB hit if the id is unchanged
    session[:current_zookeeper_id] = id if session[:current_zookeeper_id] != id
  end

  private

  # Populates the session id with your preference zookeeper id
  def set_zookeeper_with_preference
    user_zk_pref = current_user.zookeeper_preference

    if user_zk_pref.name.to_i > 0 # If your preference is not the default
      # If your preferred zookeeper doesnt exist
      if Zookeeper.find_by_id(user_zk_pref.value).nil?
        flash[:error] = "Your preferred Zookeeper no longer exists, your preference has been reset!"
        # Reset their preference to the default
        user_zk_pref.name = 0
        user_zk_pref.save
      else
        set_zookeeper user_zk_pref.value
      end
    end
  end

  # Populates the @zookeepers instance variable for option select
  def zookeepers
    @zookeepers ||= Zookeeper.order 'name'
  end

  def current_user_session
    @current_user_session ||= UserSession.find
  end

  ### Application Wide Error Handling ###
  #Locks the actions to their defined "formats"
  def lock_down_api
    action = params[:action]
    # When the format is blank it is an http request
    format = (params[:format] || :html).to_sym
    # Respond to specific format and hash of actions
    accepted_actions = mimes_for_respond_to[format]
    error = true

    # If the action doesnt respond to that format
    if !accepted_actions.nil?
      if accepted_actions[:except]
        error = accepted_actions[:except].include?(action)
      elsif accepted_actions[:only]
        error = !accepted_actions[:only].include?(action)
      else
        error = false
      end
    end

    raise "Unaccepted Format for this Action!" if error
  end

  # Error message for incorrect zookeeper find
  def zookeeper_error
    if request.xhr?
      render :status => :conflict, :text => "No Current Zookeeper"
    else
      flash[:error] = "A Zookeeper with that id does not exist!"
      redirect_to root_path
    end
  end


end
