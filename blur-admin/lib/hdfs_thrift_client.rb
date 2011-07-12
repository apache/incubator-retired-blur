class HdfsThriftClient
  def self.client
    @client = Ganapati::Client.new(HDFS_THRIFT[:host], HDFS_THRIFT[:port]) unless @client
    @client
  end
end