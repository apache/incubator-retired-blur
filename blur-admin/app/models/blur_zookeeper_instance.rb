class BlurZookeeperInstance < ActiveRecord::Base
  has_many :controllers
  has_many :clusters, :through => :controllers

  # Nested has_many :through relationships are not supported in rails (until 3.1)
  def shards
    self.clusters.collect { |cluster| cluster.shards }.flatten
  end
end
