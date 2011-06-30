class Zookeeper < ActiveRecord::Base
  has_many :controllers
  has_many :clusters
end
