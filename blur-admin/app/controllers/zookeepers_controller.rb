class ZookeepersController < ApplicationController
  before_filter :set_zookeeper_with_preference, :only => :index
  before_filter :set_zookeeper_before_filter, :only => :show
  before_filter :current_zookeeper, :only => :show

  def index
    set_zookeeper Zookeeper.first.id if Zookeeper.count == 1
    respond_to do |format|
      format.html
      format.json { render :json => Zookeeper.dashboard_stats }
    end
  end

  def show
    respond_to do |format|
      format.html
      format.json { render :json => @current_zookeeper, :methods => [:clusters, :blur_controllers] }
    end
  end

  def destroy
    zookeeper = Zookeeper.find(params[:id])
    unless zookeeper.nil?
      zookeeper.destroy
      Audit.log_event(current_user, "Zookeeper (#{zookeeper.name}) was forgotten", "zookeeper", "delete") if zookeeper.destroyed?
    end
    render :json => zookeeper
  end

  def long_running_queries
    long_queries = Zookeeper.find(params[:id])
      .blur_queries.where('created_at < ? and state = ?', 1.minute.ago, 0)
      .collect{|query| query.summary(current_user)}
    render :json => long_queries
  end
end
