class DataController < ApplicationController
  before_filter :table_name, :only => [:update, :destroy_table]

  def show
    @blur_tables = BlurTables.all
  end

  def update
    enabled = params[:enabled]
    if enabled == 'true'
      thrift_client.enableTable table_name
    elsif enabled == 'false'
      #thrift_client.disableTable table_name
    end

    render :json => thrift_client.describe(table_name).isEnabled
  end
  
  def destroy
    #client.removeTable(params[:name], params[:underlying])
    render :json => !thrift_client.tableList.include?(table_name)
  end
  
  protected
  def table_name
    table_name = params[:id]
  end
end
