class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :shards, :dependent => :destroy
  has_many :blur_tables, :dependent => :destroy, :order => 'table_name'

  attr_accessor :can_update

  def as_json(options={})
    serial_properties = super(options)
    serial_properties["can_update"] = self.can_update
    serial_properties
  end

  def has_errors?
    return self.shards.reject{|shard| shard.status == 1}.length > 0
  end
end
