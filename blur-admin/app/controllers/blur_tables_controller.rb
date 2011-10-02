class BlurTablesController < ApplicationController

  before_filter :current_zookeeper, :only => :index
  before_filter :zookeepers, :only => :index
  before_filter :table, :except => :index

  def index
    @blur_tables = @current_zookeeper.blur_tables.order('status DESC, table_name ASC')
  end

  def update
    if params[:enable]
      @table.enable
    elsif params[:disable]
      @table.disable
    end

    respond_to do |format|
      format.html { render :partial => 'blur_table', :locals => {:blur_table => @table }}
    end
  end

  def destroy
    destroy_index = params[:delete_index] == 'true'
    @table.destroy destroy_index

    respond_to do |format|
      format.js  { render :nothing => true }
    end
  end

  def schema
    respond_to do |format|
      format.html {render :partial => 'schema', :locals => {:blur_table => @table}}
    end
  end

  def hosts
    respond_to do |format|
      format.html {render :partial => 'hosts', :locals => {:blur_table => @table}}
    end
  end

  private
    def table
      @table = BlurTable.find(params[:id])
    end
end
