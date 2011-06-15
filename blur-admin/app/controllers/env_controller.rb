class EnvController < ApplicationController

  def show
    @controllers = thrift_client.controllerServerList
    @shards = thrift_client.shardServerList
  end

end
