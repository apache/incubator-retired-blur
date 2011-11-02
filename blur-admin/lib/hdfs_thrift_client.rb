require 'ganapati'
class HdfsThriftClient
  @@connections = {}

  def self.client(host, port)
    #need to do some stuff with time invalidation
    url = "#{host}:#{port}"
    #@@connections[url] ||= ThriftClient.new Ganapati::Client, url, :retries => 10
    Ganapati::Client.new host, port, nil
  end
end
