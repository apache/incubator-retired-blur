class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :blur_shards, :dependent => :destroy
  has_many :blur_tables, :dependent => :destroy, :order => 'table_name'

  attr_accessor :can_update

  def as_json(options={})
    serial_properties = super(options)
    serial_properties["can_update"] = self.can_update
    serial_properties["shard_blur_version"] = self.shard_version
    serial_properties
  end

  def shard_version
    versions = self.blur_shards.select(:blur_version).group(:blur_version)
    puts versions.inspect
    if versions.length < 1
      "No shards in this Cluster!"
    else
      versions.length == 1 ? versions.first.blur_version : "Inconsistent Blur Versions"
    end
  end

  def has_errors?
    return self.blur_shards.reject{|shard| shard.status == 1}.length > 0
  end
end
