# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
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
    raise "Cannot Remove A Shard that is online!" if @blur_shard.shard_status == 1
    @blur_shard.destroy
    Audit.log_event(current_user, "Shard (#{@blur_shard.node_name}) was forgotten",
                    "shard", "delete", @blur_shard.zookeeper) if @blur_shard.destroyed?

    respond_with(@blur_shard)
  end
end