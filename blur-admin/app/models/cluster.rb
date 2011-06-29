class Cluster < ActiveRecord::Base
  belongs_to :blur_zookeeper_instance
  has_many :shards
  has_one :blur_zookeeper_instance, :through => :controller
end
