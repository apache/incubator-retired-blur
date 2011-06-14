require "spec_helper"

describe ApplicationController do
  describe "setup_thrift" do
    it "rescues from a TransportException error" do
      host = 'string'
      port = 'string'
      @transport = Thrift::FramedTransport.new(Thrift::BufferedTransport.new(Thrift::Socket.new(BLUR_THRIFT[host], BLUR_THRIFT[port])))
      controller.stub!(:new).and_return(@transport)
      @client = mock(Blur::Blur::Client)
      #@transport.open()
    end
  end

  describe "thrift_client" do
    it "makes the setup_thrift call if @client" do
      @client = mock(Blur::Blur::Client)
      controller.stub!(:setup_thrift)
    end
  end
end