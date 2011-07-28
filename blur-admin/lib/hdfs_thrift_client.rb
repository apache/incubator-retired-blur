class HdfsThriftClient
  def self.client(hdfs_id)
    #@client = Ganapati::Client.new(HDFS_THRIFT[:host], HDFS_THRIFT[:port]) unless @client
    if clients.has_key?(hdfs_id)
      @client = clients[hdfs_id]
    else
      @client = Ganapati::Client.new(Hdfs.find(hdfs_id).host, Hdfs.find(hdfs_id).port)
      clients[hdfs_id] = @client
    end
    @client
  end
  def self.clients
    @clients = {} unless @clients
    @clients
  end
end