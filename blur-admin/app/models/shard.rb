class Shard < ActiveRecord::Base
  belongs_to :cluster
  has_and_belongs_to_many :blur_tables

  has_one :zookeeper, :through => :cluster
  has_many :blur_queries, :through => :blur_tables
end
