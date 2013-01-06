  class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :blur_shards, :dependent => :destroy
  has_many :blur_tables, :dependent => :destroy, :order => 'table_name'
  has_many :blur_queries, :through => :blur_tables, :dependent => :destroy

  attr_accessor :can_update

  def as_json(options={})
    serial_properties = super(options)
    serial_properties["can_update"] = self.can_update
    serial_properties["shard_blur_version"] = self.shard_version
    serial_properties["shard_status"] = self.shard_status
    serial_properties["cluster_queried"] = self.query_status
    serial_properties
  end

  def shard_version
    versions = self.blur_shards.select(:blur_version).group(:blur_version)
    if versions.length < 1
      "No shards in this Cluster!"
    else
      versions.length == 1 ? versions.first.blur_version : "Inconsistent Blur Versions"
    end
  end

  def shard_status
    shard_total = self.blur_shards.count
    shards_online = 0
    self.blur_shards.each do |s|
      shards_online += 1 if s.shard_status == 1
    end
    "#{shards_online} | #{shard_total}"
  end

  def query_status
    self.blur_queries.where("created_at > '#{10.minutes.ago}'").length > 0
  end
end
