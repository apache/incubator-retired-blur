class RuntimeController < ApplicationController
  require 'thrift/blur'
  def show
    client = setup_thrift
    @tables = client.tableList()
    close_thrift
  end

  def current_queries
    #TODO: get current table for quereies
    #logger.info "*** getting queries for #{params[:table]} ***"
    #@running_queries = BlurQueries.where("table_name = ?", params[:table]).all
    #render render :json => @running_queries
  end
end

