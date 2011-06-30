class Shard < ActiveRecord::Base
  belongs_to :cluster
  has_one :zookeeper, :through => :cluster

end
