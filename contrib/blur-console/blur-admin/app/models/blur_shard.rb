class BlurShard < ActiveRecord::Base
  after_destroy :destroy_parent_cluster

  belongs_to :cluster

  has_one :zookeeper, :through => :cluster

  private
  def destroy_parent_cluster
    self.cluster.destroy if self.cluster.blur_shards.count <= 0
  end
end
