class ZookeepersController < ApplicationController
  load_and_authorize_resource :only => [:index, :destroy, :long_running_queries, :show], :shallow => true

  before_filter :set_zookeeper_with_preference, :only => :index

  respond_to :html, :only => [:index, :show]
  respond_to :json

  def index
    set_zookeeper Zookeeper.first.id if Zookeeper.count == 1
    respond_with do |format|
      format.json { render :json => Zookeeper.dashboard_stats }
    end
  end

  def show
    @zookeepers = Zookeeper.all
    set_zookeeper params[:id]
    respond_with(@zookeeper) do |format|
      format.json { render :json => @zookeeper, :methods => [:clusters, :blur_controllers] }
    end
  end

  def destroy
    raise "Cannot Remove A Zookeeper that is online!" if @zookeeper.zookeeper_status == 1
    @zookeeper.destroy
    Audit.log_event(current_user, "Zookeeper (#{@zookeeper.name}) was forgotten", "zookeeper", "delete", @zookeeper) if @zookeeper.destroyed?
    respond_with(@zookeeper)
  end

  def long_running_queries
    respond_with(@zookeeper) do |format|
      format.json { render :json => @zookeeper.long_running_queries(current_user) }
    end
  end
end
