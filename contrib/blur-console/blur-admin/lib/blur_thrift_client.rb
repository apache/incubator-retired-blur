class BlurThriftClient
  @@connections = {}

  # ["host:port","host2:port"]
  def self.client(host_port)
    if host_port.class == String
      urls = host_port.split(/,/)
    else
      urls = Array(host_port)
    end
    ThriftClient.new Blur::Blur::Client, urls, :retries => 10
  end
end
