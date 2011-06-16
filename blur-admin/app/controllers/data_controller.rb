class DataController < ApplicationController
  before_filter :table_name, :only => [:update, :destroy]

  def show
    @blur_tables = BlurTables.all
  end

  def update
    enabled = params[:enabled]
    table = BlurTables.find_by_table_name(table_name)
    if enabled == 'true'
      result = table.enable
    elsif enabled == 'false'
      result = table.disable
    end

    render :json => result
  end
  
  def destroy
    table = BlurTables.find_by_table_name(table_name)
    result = table.destroy params[:underlying]
    render :json => result
  end
  
  protected
  def table_name
    table_name = params[:id]
  end
end
