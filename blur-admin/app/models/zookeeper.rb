class Zookeeper < ActiveRecord::Base
  set_table_name "blur_zookeeper_instances"
  has_many :controllers, :foreign_key => "blur_zookeeper_id"
  has_many :clusters, :foreign_key => "blur_zookeeper_id"
  has_many :shards, :through => :clusters
end
