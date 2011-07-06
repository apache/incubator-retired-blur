class Shard < ActiveRecord::Base
  belongs_to :cluster
  has_one :zookeeper, :through => :cluster
  has_many :blur_tables
  has_many :blur_queries, :through => :blur_tables
end
