class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :shards
end
