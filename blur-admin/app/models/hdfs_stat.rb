class HdfsStat < ActiveRecord::Base
  belongs_to :hdfs

  def capacity
  	self.present_capacity / 1024**3
  end

  def used
	self.dfs_used / 1024**3
  end
end
