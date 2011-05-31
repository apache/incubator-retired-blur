class RuntimeController < ApplicationController
  require 'thrift/blur'
  def show
    client = setup_thrift
    @tables = client.tableList()
    @running_queries = BlurQueries.all
    close_thrift
  end
end

