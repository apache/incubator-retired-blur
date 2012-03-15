class BlurThriftClient
  @@connections = {}

  # ["host:port","host2:port"]
  def self.client(host_port)
    if host_port.class == String
      urls = host_port.split(/,/)
    else
      urls = Array(host_port)
    end
    #blacklist_timeout = urls.size
    #blacklist_timeout = -1 if blacklist_timeout < 2
    #@@connections[urls] ||= ThriftClient.new Blur::Blur::Client, urls, :server_retry_period=> blacklist_timeout, :retries => 10
    ThriftClient.new Blur::Blur::Client, urls, :retries => 10
  end
end
