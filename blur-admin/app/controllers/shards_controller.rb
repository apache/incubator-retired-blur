class ShardsController < ApplicationController
  load_and_authorize_resource :cluster
  load_and_authorize_resource :through => :cluster, :shallow => true

  def index
    respond_to do |format|
      format.html { render_404 }
      format.json { render :json => @shards, :except => :cluster_id }
    end
  end

  def destroy
    @shard.destroy
    Audit.log_event(current_user, "Shard (#{@shard.node_name}) was forgotten", "shard", "delete") if shard.destroyed?

    respond_to do |format|
      format.html { render_404 }
      format.json { render :json => {} }
    end
  end
end