class ApplicationController < ActionController::Base
  protect_from_forgery

  before_filter :lock_down_api

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

  private
  # Populates the @current_zookeeper instance variable
  def current_zookeeper
    # Find the zookeeper with the given or the stored session id
    @current_zookeeper ||= Zookeeper.find_by_id(params[:zookeeper_id] || session[:current_zookeeper_id])
    if @current_zookeeper.nil?
      zookeeper_error
    else
      # Set the zookeeper if the value did not come from the session
      set_zookeeper @current_zookeeper.id if params[:zookeeper_id] 
    end
  end

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
        session[:current_zookeeper_id] = user_zk_pref.value
      end
    end
  end

  # Pass through for setting a zookeeper in a before filter
  def set_zookeeper_before_filter
    set_zookeeper params[:id]
  end

  def set_zookeeper(id)
    id = id.to_i # Convert all inputs to an int
    if id.nil? || Zookeeper.find_by_id(id).nil?
      zookeeper_error
    else
      session[:current_zookeeper_id] = id
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

  # A 404 rendering helper method
  def render_404
    raise ActionController::RoutingError.new('Not Found')
  end
end
