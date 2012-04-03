class HdfsMetricsController < ApplicationController
  def index
    @hdfs_index = Hdfs.all
  end
end
