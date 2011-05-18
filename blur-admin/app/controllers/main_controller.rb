class MainController < ApplicationController
  require 'thrift/blur'
  def index
    client = setup_thrift
    @tables = client.tableList()
    close_thrift
  end
  
  def view_running
    client = setup_thrift
    running_queries = client.currentQueries(params[:table])
    
   close_thrift
    render :json => running_queries
  end
  
  
end
