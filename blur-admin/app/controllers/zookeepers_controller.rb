class ZookeepersController < ApplicationController
  def show
    # If request is for a specific blur instance, update the current blur instance in session
    session[:current_zookeeper_id] = params[:id] if params[:id]

    @zookeepers = Zookeeper.all
    @zookeeper = current_zookeeper
    @controllers = @zookeeper.controllers
    @clusters = @zookeeper.clusters
    @shards = @zookeeper.shards
    respond_to do |format|
      format.html { render :partial => 'zookeeper' if request.xhr? }
    end
  end
end
