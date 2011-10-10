class BlurTablesController < ApplicationController

  before_filter :current_zookeeper, :only => [:index, :update, :destroy, :reload, :update_all, :delete_all]
  before_filter :zookeepers, :only => :index
  before_filter :table, :except => [:index, :reload, :update_all, :delete_all]

  def index
    @blur_tables = @current_zookeeper.blur_tables.order('status DESC, table_name ASC').includes('cluster')
    @clusters = @current_zookeeper.clusters.order('name')
  end
  
  def reload
    selectors = STATUS_SELECTOR[params[:status].to_sym]
    where_clause = ["status in (#{Array.new(selectors.size, '?').join(', ')}) and cluster_id = ?", selectors, params[:cluster_id]].flatten
    
    tables = @current_zookeeper.blur_tables.where(where_clause).order('table_name ASC').includes('cluster')
    render :partial => "#{params[:status]}_tables", :locals => {:tables => tables, :cluster => params[:cluster_id]}
  end

  def update
    if params[:enable]
      @table.status = STATUS[:enabling]
      @table.save
      @table.enable(@current_zookeeper.host, @current_zookeeper.port)
    elsif params[:disable]
      @table.status = STATUS[:disabling]
      @table.save
      @table.disable(@current_zookeeper.host, @current_zookeeper.port)
    end
    render :text => ''
  end
  
  def update_all
    cluster_id = params[:cluster_id]
    
    if params[:enable]
      tables = @current_zookeeper.blur_tables.where('status = ? and cluster_id =?', STATUS[:disabled], cluster_id)
      tables.each do |table|
        table.status = STATUS[:enabling]
        table.save
        table.enable(@current_zookeeper.host, @current_zookeeper.port)
      end
    elsif params[:disable]
      tables = @current_zookeeper.blur_tables.where('status = ? and cluster_id =?', STATUS[:active], cluster_id)
      tables.each do |table|
        table.status = STATUS[:disabling]
        table.save
        table.disable(@current_zookeeper.host, @current_zookeeper.port)
      end
    end
    render :text => ''
  end

  def destroy
    @table.status = STATUS[:deleting]
    @table.save
    destroy_index = params[:delete_index] == 'true'
    @table.destroy destroy_index, @current_zookeeper.host, @current_zookeeper.port
    render :text => ''
  end
  
  def delete_all
    tables = @current_zookeeper.blur_tables.where('status = ? and cluster_id =?', STATUS[:disabled], params[:cluster_id])
    destroy_index = params[:delete_index] == 'true'
    tables.each do |table|
      table.status = STATUS[:deleting]
      table.save
      table.destroy(destroy_index, @current_zookeeper.host, @current_zookeeper.port)
    end
    render :text => ''
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
    STATUS = {:enabling => 5, :active => 4, :disabling => 3, :disabled => 2, :deleteing => 1, :deleted => 0}
    STATUS_SELECTOR = {:active => [4, 3], :disabled => [2, 5, 1], :deleted => [0]}
  
    def table
      @table = BlurTable.find(params[:id])
    end
end
