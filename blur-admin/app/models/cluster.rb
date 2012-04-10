class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :shards, :dependent => :destroy
  has_many :blur_tables, :dependent => :destroy

  def has_errors?
    return self.shards.reject{|shard| shard.status == 1}.length > 0
  end
end
