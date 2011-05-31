class DataController < ApplicationController
  require 'thrift/blur'

  def show
    client = setup_thrift
    bq = Blur::BlurQuery.new
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

  def enable_table
    logger.info "*** enabling table #{params[:name]} ***"
    #TODO: Enable the table in params[:table]
    result = true
    client = setup_thrift
    render :json => result
  end

  def disable_table
    logger.info "*** disabling table #{params[:name]} ***"
    #TODO: Disable the table in params[:table]
    result = true
    render :json => result
  end

  def destroy_table 
    logger.info "*** deleting table #{params[:name]} ***"
    #TODO: Delete the table specified in params[:table]
    result = true
    render :json => result
  end
end
