class BlurThriftClient
  @@connections = {}

  def self.client(host, port)
    url = "#{host}:#{port}"
    @@connections[url] ||= ThriftClient.new Blur::Blur::Client, url, :retries => 10
  end
end
