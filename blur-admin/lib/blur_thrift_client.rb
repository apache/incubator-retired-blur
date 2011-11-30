class BlurThriftClient
  @@connections = {}

  # ["host:port","host2:port"]
  def self.client(host_port)
    urls = Array(host_port)
    blacklist_timeout = urls.size
    blacklist_timeout = -1 if blacklist_timeout < 2
    @@connections[urls] ||= ThriftClient.new Blur::Blur::Client, urls, :server_retry_period=> blacklist_timeout, :retries => 10
  end
end
