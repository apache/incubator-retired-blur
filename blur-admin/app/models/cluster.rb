class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :shards
  has_one :zookeeper, :through => :controller
end
