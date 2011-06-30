class Shard < ActiveRecord::Base
  belongs_to :cluster
  has_one :controller, :through => :cluster

  # Nested has_many :through relationships are not supported in rails (until 3.1)
  def zookeeper
    self.controller.zookeeper
  end
end
