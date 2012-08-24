class ClustersController < ApplicationController
  load_and_authorize_resource

  def destroy
    @cluster.destroy
    Audit.log_event(current_user, "Cluster (#{@cluster.name}) was forgotten", "cluster", "delete") if @cluster.destroyed?

    respond_to do |format|
      format.html { render_404 }
      format.json { render :json => {} }
    end
  end
end