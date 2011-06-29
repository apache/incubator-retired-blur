class Controller < ActiveRecord::Base
  belongs_to :blur_zookeeper_instance
  has_many :clusters
  has_many :shards, :through => :clusters
end
