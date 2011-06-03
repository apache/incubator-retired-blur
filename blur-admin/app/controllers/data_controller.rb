class DataController < ApplicationController
  before_filter :setup_thrift
  after_filter :close_thrift
  
  def show
    bq = Blur::BlurQuery.new
    bq.queryStr = '*'
		bq.fetch = 1
    bq.superQueryOn = false
    @tables = client.tableList.sort
    @tdesc = {}
    @tschema = {}
    @tserver = {}
    @tcount = {}
    @tables.each do |table|
      @tdesc[table] = @client.describe(table)
      @tschema[table] = @client.schema(table).columnFamilies
      @tserver[table] = @client.shardServerLayout(table)
      @tcount[table] = @client.query(table, bq).totalResults
    end
  end

  def enable_table
    logger.info "*** enabling table #{params[:name]} ***"
    @client.enableTable(params[:name])
    render :json => @client.describe(params[:name]).isEnabled
  end

  def disable_table
    logger.info "*** disabling tabldeleteIndexFilese #{params[:name]} ***"
    @client.disableTable(params[:name])
    render :json => !@client.describe(params[:name]).isEnabled
  end

  def destroy_table 
    logger.info "*** deleting table #{params[:name]} ***"
    # TODO: Uncomment below when we can create a table
    #client.removeTable(params[:name], false)
    render :json => !@client.tableList.include? params[:name]
  end
end
