class Cluster < ActiveRecord::Base
  belongs_to :controller
  has_many :shards
  has_one :blur_zookeeper_instance, :through => :controller
end
