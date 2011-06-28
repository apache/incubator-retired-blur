class BlurZookeeperInstancesController < ApplicationController
  def show
    # If request is for a specific blur instance, update the current blur instance in session
    session[:current_blur_zookeeper_instance_id] = params[:id] if params[:id]

    @blur_zookeeper_instances = BlurZookeeperInstance.all
    @controllers = current_blur_zookeeper_instance.controllers
    @clusters = @controllers.collect {|controller| controller.clusters}.flatten
    @shards = @clusters.collect{|cluster| cluster.shards}.flatten
    respond_to do |format|
      format.html { render :partial => 'blur_zookeeper_instance' if request.xhr? }
    end
  end
end
