class BlurThriftClient
  def self.client
    @client = ThriftClient.new(Blur::Blur::Client, "#{BLUR_THRIFT[:host]}:#{BLUR_THRIFT[:port]}", :retries => 10) unless @client
    @client
  end
end