require 'ganapati'
class HdfsThriftClient
  @@connections = {}

  def self.client(host, port)
    #need to do some stuff with time invalidation
    url = "#{host}:#{port}"
    @@connections[url] ||= Ganapati::Client.new host, port
  end
end
