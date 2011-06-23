class BlurTablesController < ApplicationController
  def index
    @blur_tables = BlurTable.all
  end

  def update
    @blur_table = BlurTable.find(params[:id])
    if params[:enable]
      @blur_table.enable
    elsif params[:disable]
      @blur_table.disable
    end

    respond_to do |format|
      format.html { render :partial => 'blur_table', :locals => {:blur_table => @blur_table }}
    end
  end

  def destroy
    @blur_table = BlurTable.find(params[:id])
    destroy_index = params[:delete_index] == 'true'
    @blur_table.destroy destroy_index

    respond_to do |format|
      format.js  { render :nothing => true }
    end
  end

  def schema
    @schema = BlurTable.find(params[:id]).schema

    respond_to do |format|
      format.html { render :partial => 'schema', :locals => {:schema => @schema} }
    end
  end
end
