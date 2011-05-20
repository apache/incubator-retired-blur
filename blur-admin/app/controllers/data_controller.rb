class DataController < ApplicationController

  require 'thrift/blur'
  def show
    client = setup_thrift
    @tables = client.tableList()
    close_thrift
  end

  def curr_tables
    client = setup_thrift
    curr_tables = client.tableList()
    close_thrift

    render :json => curr_tables
  end

end
