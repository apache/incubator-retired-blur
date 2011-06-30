class Zookeeper < ActiveRecord::Base
  has_many :controllers
  has_many :clusters
  has_many :shards, :through => :clusters
end
