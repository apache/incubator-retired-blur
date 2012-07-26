class Zookeeper < ActiveRecord::Base
  has_many :controllers, :dependent => :destroy
  has_many :clusters, :dependent => :destroy
  has_many :shards, :through => :clusters
  has_many :blur_tables, :through => :clusters
  has_many :blur_queries, :through => :blur_tables

  def translated_status
    case read_attribute(:status)
      when 0 then "Offline"
      when 1 then "Online"
    end
  end

end
