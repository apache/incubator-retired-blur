class DataController < ApplicationController
  before_filter :setup_thrift
  before_filter :table_name, :only => [:update, :destroy_table]
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

  #TODO: Add feedback to enable / disable on view
  def update
    enabled = params[:enabled]
    if enabled == 'true'
      @client.enableTable table_name
    elsif enabled == 'false'
      #@client.disableTable table_name
    end

    render :json => @client.describe(table_name).isEnabled
  end

  #TODO: Add feedback to delete button on view
  def destroy
    if underlying
      #client.removeTable(params[:name], true)
    # TODO: Uncomment below when we can create a table
    else
      #client.removeTable(params[:name], false)
    end
    render :json => !@client.tableList.include?(table_name)
  end

  protected

  def table_name
    table_name = params[:id]
  end

end
