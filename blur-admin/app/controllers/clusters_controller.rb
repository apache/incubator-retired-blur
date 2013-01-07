class ClustersController < ApplicationController
  load_and_authorize_resource

  respond_to :json

  def destroy
    raise "Cannot Remove A Cluster that is online!" if @cluster.cluster_status == 1
    @cluster.destroy
    Audit.log_event(current_user, "Cluster (#{@cluster.name}) was forgotten",
                    "cluster", "delete", @cluster.zookeeper) if @cluster.destroyed?
    respond_with(@cluster)
  end
end