class EnvController < ApplicationController

  after_filter :close_thrift

  def show
    @controllers = thrift_client.controllerServerList
    @shards = thrift_client.shardServerList
  end

end
