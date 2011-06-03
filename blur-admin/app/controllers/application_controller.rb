class ApplicationController < ActionController::Base
  protect_from_forgery

	require 'thrift/blur'

  def setup_thrift
    @transport = Thrift::FramedTransport.new(Thrift::BufferedTransport.new(Thrift::Socket.new(BLUR_THRIFT[:host], BLUR_THRIFT[:port])))
    protocol = Thrift::BinaryProtocol.new(@transport)
    @client = Blur::Blur::Client.new(protocol)
    @transport.open()
  rescue Thrift::TransportException
    @client = nil
  end
  
  def close_thrift
    @transport.close()
  end
end
