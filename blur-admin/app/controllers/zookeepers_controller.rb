class ZookeepersController < ApplicationController

  before_filter :current_zookeeper, :only => :show_current
  before_filter :zookeepers, :only => [:show_current, :index]

  QUERY = "
    select
      z.name,
    	z.status,
    	z.id,
    	v.controller_version,
    	c.controller_offline_node,
    	c.controller_disabled_node,
    	c.controller_total,
    	v.shard_version,
    	s.shard_offline_node,
    	s.shard_disabled_node,
    	s.shard_total
    from
    	zookeepers z,
    	(select z1.id, count(distinct c1.blur_version) as controller_version, count(distinct s1.blur_version) as shard_version from zookeepers z1 left join controllers c1 on (z1.id = c1.zookeeper_id), zookeepers z2 left join clusters c2 on (z2.id = c2.zookeeper_id) left join shards s1 on (c2.id = s1.cluster_id) where z1.id = z2.id group by z1.name) v,
    	(select z2.id, sum(if(c3.status = 0, 1, 0)) as controller_offline_node, sum(if(c3.status = 1, 1, 0)) as controller_disabled_node, count(c3.id) as controller_total from zookeepers z2 left join controllers c3 on (z2.id = c3.zookeeper_id) group by z2.name) c,
    	(select z3.id, sum(if(s2.status = 0, 1, 0)) as shard_offline_node, sum(if(s2.status = 1, 1, 0)) as shard_disabled_node, count(s2.id) as shard_total from zookeepers z3 left join clusters c4 on (z3.id = c4.zookeeper_id) left join shards s2 on (c4.id = s2.cluster_id) group by z3.name) s
    where
    	z.id = v.id and
    	z.id = c.id and
    	z.id = s.id
    order by
    	z.name
  "
  
  def index
    @zookeepers = Zookeeper.select('id, name')
  end
  
  def show_current
    @zookeeper = @current_zookeeper

    @shard_nodes = @zookeeper.shards.collect { |shard| shard.blur_version }.flatten.uniq.length
    @controller_nodes = @zookeeper.controllers.collect { |controller| controller.blur_version }.flatten.uniq.length

    respond_to do |format|
      format.html { render :show_current }
    end
  end

  def make_current
    session[:current_zookeeper_id] = params[:id] if params[:id]

    # Javascript redirect (has to be done in js)
    render :js => "window.location = '#{request.referer}'"
  end
  
  def dashboard
    connection = ActiveRecord::Base.connection()

    zookeeper_results = []
    connection.execute(QUERY).each_hash { |row| zookeeper_results << row }

    time = Time.zone.now - 1.minutes
    data = {
      :zookeepers => zookeeper_results, #Zookeeper.includes(:controllers, :clusters=>[:shards]),
      :long_queries => BlurQuery.where(['created_at < ? and running = 1', time]).count
    }
    
    respond_to do |format|
      format.json { render :json => data.to_json() }
    end
  end
end
