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
    #@tisEnabled = Hash.new
    @tables.each do |table|
      @tdesc[table] = client.describe(table)
      @tschema[table] = client.schema(table).columnFamilies
      @tserver[table] = client.shardServerLayout(table)
      @tcount[table] = client.query(table, bq).totalResults
      #@tisEnabled[table] = @tables.
    end
    close_thrift
  end

  def action
    client = setup_thrift
    logger.info "#{params[:action_type]} table #{params[:table]}"

    if params[:action_type] == "enable"
      #TODO: Enable the table in params[:table]
    elsif params[:action_type] == "disable"
      #TODO: Disable the table in params[:table]
    elsif params[:action_type] == "delete"
      #TODO: Delete the table in params[:table]
    end
    
    result = true
    render :json => result
  end
end

