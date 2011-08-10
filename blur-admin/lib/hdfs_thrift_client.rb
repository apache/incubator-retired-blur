require 'ganapati'
class HdfsThriftClient
  @@connections = {}

  def self.client(host, port)
    url = "#{host}:#{port}"
    @@connections[url] ||= Ganapati::Client.new host, port
  end
end
