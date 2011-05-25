class DataController < ApplicationController

  require 'thrift/blur'
  def show
    client = setup_thrift
    bq = BG::BlurQuery.new
    bq.queryStr = '*'
		bq.fetch = 1
    bq.superQueryOn = false
    @tables = client.tableList()
    @tdesc = Hash.new
    @tschema = Hash.new
    @tserver = Hash.new
    @tcount = Hash.new
    @tables.each do |table|
      @tdesc[table] = client.describe(table)
      @tschema[table] = client.schema(table).columnFamilies
      @tserver[table] = client.shardServerLayout(table)
      @tcount[table] = client.query(table, bq).totalResults
    end
    close_thrift
  end

  def enable 
    client = setup_thrift
    logger.info "Enabling Table #{params[:table]}"
    #TODO: Enable the table in params[:table]
  end

  def disable
    client = setup_thrift
    logger.info "Disabling Table #{params[:table]}"
    #TODO: Disable the table in params[:table]
  end

end

