class ZookeepersController < ApplicationController
  def show
    # If request is for a specific blur instance, update the current blur instance in session
    session[:current_zookeeper_id] = params[:id] if params[:id]

    @zookeepers = Zookeeper.all
    @controllers = current_zookeeper.controllers
    @clusters = current_zookeeper.clusters
    @shards = @clusters.collect{|cluster| cluster.shards}.flatten
    respond_to do |format|
      format.html { render :partial => 'zookeeper' if request.xhr? }
    end
  end
end
