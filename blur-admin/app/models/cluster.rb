class Cluster < ActiveRecord::Base
  belongs_to :zookeeper
  has_many :shards
  has_many :blur_tables, :through => :shards

  #rails 3.0 does not allow nested has_many :through relationships
  def blur_queries
    self.blur_tables.collect { |blur_table| blur_table.blur_queries }.flatten
  end
end
