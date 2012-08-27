class ClustersController < ApplicationController
  load_and_authorize_resource

  respond_to :json

  def destroy
    @cluster.destroy
    Audit.log_event(current_user, "Cluster (#{@cluster.name}) was forgotten",
                    "cluster", "delete") if @cluster.destroyed?
    respond_with(@cluster)
  end
end