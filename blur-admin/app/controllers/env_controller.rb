class EnvController < ApplicationController

  def show
    setup_thrift
    @controllers = @client.controllerServerList
    @shards = @client.shardServerList
    close_thrift
  end

end
