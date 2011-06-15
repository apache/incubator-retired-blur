class DataController < ApplicationController
  before_filter :table_name, :only => [:update, :destroy]

  def show
    @blur_tables = BlurTables.all
  end

  def update
    enabled = params[:enabled]
    table = BlurTables.find_by_table_name(table_name)
    if enabled == 'true'
      table.enable
    elsif enabled == 'false'
      table.disable
    end

    render :json => table.is_enabled?
  end
  
  def destroy
    table = BlurTables.find_by_table_name(table_name)
    # TODO: uncomment line below when ready to destroy tables 
    #client.removeTable(params[:name], params[:underlying])
    table.destroy params[:underlying]
    render :json => true # change based on result of .destroy
  end
  
  protected
  def table_name
    table_name = params[:id]
  end
end
