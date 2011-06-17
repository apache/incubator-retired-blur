class EnvController < ApplicationController

  def show
    begin
      @controllers = BlurThriftClient.client.controllerServerList
    rescue
      @controllers = nil
    end

    begin
      @shards = BlurThriftClient.shardServerList
    rescue
      @shards = nil
    end
  end

end
