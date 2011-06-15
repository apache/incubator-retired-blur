class DataController < ApplicationController
  before_filter :table_name, :only => [:update, :destroy_table]

  def show
    @blur_tables = BlurTables.all
  end

  def update
    enabled = params[:enabled]
    if enabled == 'true'
      BlurTables.find_by_table_name(table_name).enable
    elsif enabled == 'false'
      # TODO: uncomment line below when thrift does not error
      # when trying to access disabled tables
      #BlurTables.find_by_table_name(table_name).disable
    end

    render :json => thrift_client.describe(table_name).isEnabled
  end
  
  def destroy
    # TODO: uncomment line below when ready to destroy tables 
    #client.removeTable(params[:name], params[:underlying])
    render :json => !thrift_client.tableList.include?(table_name)
  end
  
  protected
  def table_name
    table_name = params[:id]
  end
end
