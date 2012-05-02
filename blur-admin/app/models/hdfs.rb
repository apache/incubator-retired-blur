class Hdfs < ActiveRecord::Base
  has_many :hdfs_stats

  def most_recent_stats
    self.hdfs_stats.last
  end
end