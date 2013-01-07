class Zookeeper < ActiveRecord::Base
  has_many :blur_controllers, :dependent => :destroy
  has_many :clusters, :dependent => :destroy
  has_many :blur_shards, :through => :clusters
  has_many :blur_tables, :through => :clusters
  has_many :blur_queries, :through => :blur_tables

  QUERY = "
    select
      z.name,
      z.zookeeper_status,
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
      (select z1.id, count(distinct c1.blur_version) as controller_version, count(distinct s1.blur_version) as shard_version from zookeepers z1 left join blur_controllers c1 on (z1.id = c1.zookeeper_id), zookeepers z2 left join clusters c2 on (z2.id = c2.zookeeper_id) left join blur_shards s1 on (c2.id = s1.cluster_id) where z1.id = z2.id group by z1.id) v,
      (select z2.id, CAST(sum(if((c3.controller_status = 0 or c3.controller_status = 2), 1, 0)) AS SIGNED) as controller_offline_node, count(c3.id) as controller_total from zookeepers z2 left join blur_controllers c3 on (z2.id = c3.zookeeper_id) group by z2.id) c,
      (select z3.id, CAST(sum(if((s2.shard_status = 0 or s2.shard_status = 2), 1, 0)) AS SIGNED) as shard_offline_node, count(s2.id) as shard_total from zookeepers z3 left join clusters c4 on (z3.id = c4.zookeeper_id) left join blur_shards s2 on (c4.id = s2.cluster_id) group by z3.id) s,
      (select z4.id, CAST(sum(if(q1.state = 0 and q1.created_at < date_sub(utc_timestamp(), interval 1 minute), 1, 0)) AS SIGNED) as long_running_queries from zookeepers z4 left join clusters c5 on (z4.id = c5.zookeeper_id) left join blur_tables t1 on (c5.id = t1.cluster_id) left join blur_queries q1 on (t1.id = q1.blur_table_id) group by z4.id) q
    where
      z.id = v.id and
      z.id = c.id and
      z.id = s.id and
      z.id = q.id
    order by
      z.id
  "

  def as_json(options={})
    serial_properties = super(options)
    serial_properties.delete('online_ensemble_nodes')
    serial_properties['ensemble'] = JSON.parse self.online_ensemble_nodes
    serial_properties
  end

  def self.dashboard_stats
    zookeeper_results = []
    connection = ActiveRecord::Base.connection()
    connection.execute(QUERY).each(:as => :hash) { |row| zookeeper_results << row }
    zookeeper_results
  end

  def refresh_queries(lower_range)
    self.blur_queries.where("blur_queries.updated_at > ? and blur_tables.table_status = ?", lower_range, 4)
  end

  def long_running_queries(current_user)
    self.blur_queries.where('created_at < ? and state = ?', 1.minute.ago, 0).collect{|query| query.summary(current_user)}
  end
end
