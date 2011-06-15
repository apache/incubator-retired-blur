class DataController < ApplicationController
  before_filter :table_name, :only => [:update, :destroy_table]

  def show
    bq = Blur::BlurQuery.new :queryStr => '*', :fetch => 1, :superQueryOn => false
    @tables = thrift_client.tableList.sort
    @tdesc = {}
    @tschema = {}
    @tserver = {}
    @tcount = {}
    @tables.each do |table|
      @tdesc[table] = thrift_client.describe(table)
      @tschema[table] = thrift_client.schema(table).columnFamilies
      @tserver[table] = thrift_client.shardServerLayout(table)
      # @tcount[table] = thrift_client.query(table, bq).totalResults
    end
  end

  #TODO: Add feedback to enable / disable on view
  def update
    enabled = params[:enabled]
    if enabled == 'true'
      thrift_client.enableTable table_name
    elsif enabled == 'false'
      #thrift_client.disableTable table_name
    end

    render :json => thrift_client.describe(table_name).isEnabled
  end

  #TODO: Add feedback to delete button on view
  def destroy
    #client.removeTable(params[:name], params[:underlying])
    render :json => !thrift_client.tableList.include?(table_name)
  end

  protected

  def table_name
    table_name = params[:id]
  end

end
