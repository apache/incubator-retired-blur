class HdfsThriftClient
  def self.client#(hdfs_id)
    #@client = Ganapati::Client.new(HDFS_THRIFT[:host], HDFS_THRIFT[:port]) unless @client
    #@client = Ganapati::Client.new(HdfsStats.find(hdfs_id).host, HdfsStats.find(hdfs_id).port) unless @client
    puts '****hello****'
    if clients.has_key?(hdfs_id)
      @client = clients[hdfs_id]
    else
      @client = Ganapati::Client.new(HdfsStats.find(hdfs_id).host, HdfsStats.find(hdfs_id).port)
      clients[hdfs_id] = @client
    end
    @client
  end
  def clients
    {}
  end
end