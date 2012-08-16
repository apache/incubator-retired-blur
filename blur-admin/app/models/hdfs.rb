class Hdfs < ActiveRecord::Base
  has_many :hdfs_stats

  def most_recent_stats
    self.hdfs_stats.last
  end

  def recent_stats
    return false unless self.hdfs_stats.last
    self.hdfs_stats.last.created_at > 1.minute.ago
  end
end
