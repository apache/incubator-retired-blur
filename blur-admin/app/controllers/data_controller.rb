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

  def enable_table
    logger.info "*** enabling table #{params[:table]} ***"
    #TODO: Enable the table in params[:table]
    result = true
    render :json => result
  end

  def disable_table
    logger.info "*** disabling table #{params[:table]} ***"
    #TODO: Disable the table in params[:table]
    result = true
    render :json => result
  end

  def delete_table
    logger.info "*** deleting table #{params[:table]} ***"
    #TODO: Delete the table specified in params[:table]
    result = true
    render :json => result
  end

  def create_table
    logger.info "*** creating table with params: #{params} ***"
    #TODO: Create a table with parameters stored in params[]
    result = true
    render :json => result
  end

end

