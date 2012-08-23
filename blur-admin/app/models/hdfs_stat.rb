class HdfsStat < ActiveRecord::Base
  belongs_to :hdfs

  def capacity
  	self.present_capacity.to_f / 1024**3
  end

  def used
	  self.dfs_used_real.to_f / 1024**3
  end
end
