class BlurShardsController < ApplicationController
  load_and_authorize_resource :cluster
  load_and_authorize_resource :through => :cluster, :shallow => true

  respond_to :json

  def index
    respond_with(@blur_shards) do |format|
      format.json { render :json => @blur_shards, :except => :cluster_id }
    end
  end

  def destroy
    raise "Cannot Remove A Shard that is online!" if @blur_shard.status == 1
    @blur_shard.destroy
    Audit.log_event(current_user, "Shard (#{@blur_shard.node_name}) was forgotten",
                    "shard", "delete") if @blur_shard.destroyed?

    respond_with(@blur_shard)
  end
end