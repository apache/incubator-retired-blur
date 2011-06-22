class BlurTablesController < ApplicationController
  def index
    @blur_tables = BlurTable.all
  end

  def update
    @blur_table = BlurTable.find(params[:id])
    if params[:enable]
      result = @blur_table.enable
    elsif params[:disable]
      result = @blur_table.disable
    end

    respond_to do |format|
      format.js { render :partial => 'blur_table' }
    end
  end

  def destroy
    table = BlurTable.find(params[:id])
    result = table.destroy params[:underlying]
    respond_to do |format|
      format.js  { render :nothing => true }
    end
  end
end
