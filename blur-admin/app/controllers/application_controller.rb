class ApplicationController < ActionController::Base
  protect_from_forgery

	require 'thrift/blur'
  BG = Com::Nearinfinity::Blur::Thrift::Generated

  def setup_thrift
    @transport = Thrift::FramedTransport.new(Thrift::BufferedTransport.new(Thrift::Socket.new(BLUR_THRIFT[:host], BLUR_THRIFT[:port])))
    protocol = Thrift::BinaryProtocol.new(@transport)
    client = BG::Blur::Client.new(protocol)
    @transport.open()
    
    client
    
  rescue Thrift::TransportException
    client = nil
    client
  end
  
  def close_thrift
    @transport.close()
  end
end
