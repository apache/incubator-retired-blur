class Zookeeper < ActiveRecord::Base
  has_many :controllers, :dependent => :destroy
  has_many :clusters, :dependent => :destroy
  has_many :shards, :through => :clusters
  has_many :blur_tables, :through => :clusters
  has_many :blur_queries, :through => :blur_tables
end
