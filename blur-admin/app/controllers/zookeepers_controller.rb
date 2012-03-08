class ZookeepersController < ApplicationController

  before_filter :current_zookeeper, :only => [:show_current]
  before_filter :zookeepers, :only => [:show_current]
  before_filter :reset_zookeeper, :only => [:show, :make_current]

  QUERY = "
    select
      z.name,
      z.status,
      z.id,
      v.controller_version,
      c.controller_offline_node,
      c.controller_total,
      v.shard_version,
      s.shard_offline_node,
      s.shard_total,
      q.long_running_queries
    from
      zookeepers z,
      (select z1.id, count(distinct c1.blur_version) as controller_version, count(distinct s1.blur_version) as shard_version from zookeepers z1 left join controllers c1 on (z1.id = c1.zookeeper_id), zookeepers z2 left join clusters c2 on (z2.id = c2.zookeeper_id) left join shards s1 on (c2.id = s1.cluster_id) where z1.id = z2.id group by z1.id) v,
      (select z2.id, sum(if(c3.status = 0, 1, 0)) as controller_offline_node, count(c3.id) as controller_total from zookeepers z2 left join controllers c3 on (z2.id = c3.zookeeper_id) group by z2.id) c,
      (select z3.id, sum(if(s2.status = 0, 1, 0)) as shard_offline_node, count(s2.id) as shard_total from zookeepers z3 left join clusters c4 on (z3.id = c4.zookeeper_id) left join shards s2 on (c4.id = s2.cluster_id) group by z3.id) s,
      (select z4.id, sum(if(q1.state = 0 and q1.created_at < date_sub(utc_timestamp(), interval 1 minute), 1, 0)) as long_running_queries from zookeepers z4 left join clusters c5 on (z4.id = c5.zookeeper_id) left join blur_tables t1 on (c5.id = t1.cluster_id) left join blur_queries q1 on (t1.id = q1.blur_table_id) group by z4.id) q
    where
      z.id = v.id and
      z.id = c.id and
      z.id = s.id and
      z.id = q.id
    order by
      z.id
  "

  def index
    @zookeepers = Zookeeper.select('name, id, status').order('name')
    @hdfs_all = Hdfs.all
    @hdfs_stats= Hdfs.all.collect{|h| hdfs_hash = {"hdfs" => h}; hdfs_hash['stats'] = h.hdfs_stats.last; h= hdfs_hash}
  end

  def show
    redirect_to :zookeeper
  end

  def show_current
    @zookeeper = @current_zookeeper
    @shard_nodes = @zookeeper.shards.count 'DISTINCT blur_version'
    @controller_nodes = @zookeeper.controllers.count 'DISTINCT blur_version'
    render :show_current
  end

  def make_current
    # Javascript redirect (has to be done in js)
    render :js => "window.location = '#{request.referer}'"
  end

  def dashboard
    zookeeper_results = []
    connection = ActiveRecord::Base.connection()
    connection.execute(QUERY).each(:as => :hash) { |row| zookeeper_results << row }
    hdfs= Hdfs.all.collect do |h|
        hdfs_hash = JSON(h.to_json)
        hdfs_hash['stats'] = JSON(h.hdfs_stats.last.to_json) unless h.hdfs_stats.blank?
        hdfs_hash
    end
    render :json => {"zookeeper_data" => zookeeper_results, "hdfs_data" => hdfs}
  end

  def destroy_shard
    Shard.destroy(params[:shard_id])
    redirect_to :zookeeper
  end
  
  def destroy_cluster
    Cluster.destroy(params[:cluster_id])
    redirect_to :zookeeper
  end

  def destroy_controller
    Controller.destroy(params[:controller_id])
    redirect_to :zookeeper
  end
  
  def destroy_zookeeper
    Zookeeper.destroy(params[:id])
    redirect_to :zookeeper
  end

  private
    def reset_zookeeper
      session[:current_zookeeper_id] = params[:id] if params[:id]
    end
end
