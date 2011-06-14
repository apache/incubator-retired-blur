class DataController < ApplicationController
  before_filter :table_name, :only => [:update, :destroy_table]
  before_filter :setup_thrift, :only => [:show]
  after_filter :close_thrift

  def show
    @blur_tables = BlurTables.all
    @blur_tables.each do |table|
      table.describe= thrift_client.describe(table.table_name)
      table.schema= thrift_client.schema(table.table_name).columnFamilies
      table.server= thrift_client.shardServerLayout(table.table_name)
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
