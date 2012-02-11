class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :shards, :dependent => :destroy
  has_many :blur_tables, :dependent => :destroy
  has_many :blur_queries, :through => :blur_tables
end
