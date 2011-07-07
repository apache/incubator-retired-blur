class Zookeeper < ActiveRecord::Base
  has_many :controllers
  has_many :clusters
  has_many :shards, :through => :clusters
  has_many :blur_tables, :through => :clusters
  
  #rails 3.0 does not allow nested has_many :through relationships
  def blur_queries
    self.blur_tables.collect { |blur_table| blur_table.blur_queries }.flatten
  end
end
