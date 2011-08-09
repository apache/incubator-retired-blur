class BlurThriftClient
  @@connections = {}

  def self.client(url)
    @@connections[url] ||= ThriftClient.new Blur::Blur::Client, url, :retries => 10
  end
end
