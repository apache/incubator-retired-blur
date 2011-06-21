class BlurTablesController < ApplicationController
  before_filter :table_name, :only => [:update, :destroy]

  def index
    @blur_tables = BlurTable.all
  end

  def update
    enabled = params[:enabled]
    table = BlurTable.find_by_table_name(table_name)
    if enabled == 'true'
      result = table.enable
    elsif enabled == 'false'
      result = table.disable
    end

    render :json => result
  end

  def destroy
    table = BlurTable.find_by_table_name(table_name)
    result = table.destroy params[:underlying]
    render :json => result
  end
  
  protected
  def table_name
    table_name = params[:id]
  end
end
