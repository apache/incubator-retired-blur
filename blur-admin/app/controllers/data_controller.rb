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

  #def get_shards(table)
    #TODO: get the shards
    #return 'placeholder'
  #end

  #def get_size(table)
    #TODO: get the size
    #return 'placeholder'
  #end

  #def get_count(table)
    #TODO: get the count
    #return 'placeholder'
  #end

end
