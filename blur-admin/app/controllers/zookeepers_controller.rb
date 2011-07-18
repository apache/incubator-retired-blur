class ZookeepersController < ApplicationController

  before_filter :current_zookeeper, :only => :show_current
  before_filter :zookeepers, :only => [:show_current, :index]

  def show_current
    @zookeeper = @current_zookeeper

    @shard_nodes = @zookeeper.shards.collect { |shard| shard.blur_version }.flatten.uniq.length
    @controller_nodes = @zookeeper.controllers.collect { |controller| controller.blur_version }.flatten.uniq.length

    respond_to do |format|
      format.html { render :show_current }
    end
  end

  def index
    time = Time.zone.now - 1.minutes
    @old_queries = BlurQuery.where ['created_at < ? and running = 1', time]
    puts @old_queries.inspect
  end

  def make_current
    session[:current_zookeeper_id] = params[:id] if params[:id]

    # Javascript redirect (has to be done in js)
    render :js => "window.location = '#{request.referer}'"
  end
  
  def dashboard
    time = Time.zone.now - 1.minutes
    data = {
      :zookeepers => Zookeeper.includes(:controllers, :clusters=>[:shards]),
      :long_queries => BlurQuery.where(['created_at < ? and running = 1', time])
    }
    
    respond_to do |format|
      format.json { render :json => data.to_json(:include=>[ :controllers, :clusters, :shards ] ) }
    end
  end
end
