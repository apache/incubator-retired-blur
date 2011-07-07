class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :shards
  has_many :blur_tables
  has_many :blur_queries, :through => :blur_tables
end
