class MainController < ApplicationController
  require 'thrift/blur'
  def index
    client = setup_thrift
    @tables = client.tableList()
    @transport.close()
  end
  
  def view_running
    client = setup_thrift
    running_queries = client.currentQueries(params[:table])
    
    @transport.close()
    render :json => running_queries
  end
  
  def setup_thrift
    @transport = Thrift::FramedTransport.new(Thrift::BufferedTransport.new(Thrift::Socket.new('blur04.nearinfinity.com', 40020)))
    protocol = Thrift::BinaryProtocol.new(@transport)
    client = Com::Nearinfinity::Blur::Thrift::Generated::Blur::Client.new(protocol)
    @transport.open()
    
    client
  end
end
