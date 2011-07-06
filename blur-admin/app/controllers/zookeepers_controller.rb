class ZookeepersController < ApplicationController

  before_filter :current_zookeeper, :only => :show_current
  before_filter :zookeepers, :only => [:show_current, :index]

  def show_current
    @zookeeper = @current_zookeeper

    respond_to do |format|
      format.html { render :show_current }
    end
  end

  def index
  end

  def make_current
    session[:current_zookeeper_id] = params[:id] if params[:id]

    # Javascript redirect (has to be done in js)
    render :js => "window.location = '#{request.referer}'"
  end
end
