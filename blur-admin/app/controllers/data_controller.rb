class DataController < ApplicationController
  before_filter :setup_thrift
  before_filter :table_name, :only => [:enable_table, :disable_table, :destroy_table]
  after_filter :close_thrift

  def show
    bq = Blur::BlurQuery.new :queryStr => '*', :fetch => 1, :superQueryOn => false
    @tables = @client.tableList.sort
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
    @client.enableTable table_name
    render :json => @client.describe(table_name.isEnabled)
  end

  def disable_table
    @client.disableTable table_name
    render :json => !@client.describe(table_name.isEnabled)
  end

  def destroy_table 
    # TODO: Uncomment below when we can create a table
    #client.removeTable(params[:name], false)
    render :json => !@client.tableList.include?(table_name)
  end

  protected

  def table_name
    table_name = params[:name]
  end

end
