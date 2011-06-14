class EnvController < ApplicationController

  after_filter :close_thrift

  def show
    setup_thrift
    @controllers = @client.controllerServerList
    @shards = @client.shardServerList
    close_thrift
  end

end
