class BlurTablesController < ApplicationController

  before_filter :current_zookeeper, :only => [:index, :reload, :enable, :disable, :destroy, :reload, :forget, :terms]
  before_filter :zookeepers, :only => :index

  def index
    @blur_tables = @current_zookeeper.blur_tables.order('status DESC, table_name ASC').includes('cluster')
    @clusters = @current_zookeeper.clusters.order('name')
  end
  
  def reload
    render_table_json
  end

  def enable
    params[:tables].map {|table| BlurTable.find(table)}.each do |table|
      table.status = STATUS[:enabling]
      table.save
      table.enable @current_zookeeper.blur_urls
    end
    render_table_json
  end

  def disable
    params[:tables].map {|table| BlurTable.find(table)}.each do |table|
      table.status = STATUS[:disabling]
      table.save
      table.disable @current_zookeeper.blur_urls
    end
    render_table_json
  end

  def destroy
    params[:tables].map {|table| BlurTable.find(table)}.each do |table|
      table.status = STATUS[:deleting]
      table.save
      destroy_index = params[:delete_index] == 'true'
      table.blur_destroy destroy_index, @current_zookeeper.blur_urls
    end
    render_table_json
  end
  
  def forget
    params[:tables].each do |table|
      BlurTable.destroy table
    end
    render_table_json
  end

  def schema
    respond_to do |format|
      format.html {render :partial => 'schema', :locals => {:blur_table => BlurTable.find(params[:id])}}
    end
  end

  def hosts
    respond_to do |format|
      format.html {render :partial => 'hosts', :locals => {:blur_table => BlurTable.find(params[:id])}}
    end
  end

  def terms
      puts 'TEST'
      puts params.inspect
      table = BlurTable.find(params[:id])
      terms = table.terms @current_zookeeper.blur_urls, params[:family], params[:column], params[:startWith], params[:size].to_i
      render :json => terms
  end
      
  private
    STATUS = {:enabling => 5, :active => 4, :disabling => 3, :disabled => 2, :deleting => 1, :deleted => 0}
    STATUS_SELECTOR = {:active => [4, 3], :disabled => [2, 5, 1], :deleted => [0]}
    
    def render_table_json
      tables = @current_zookeeper.blur_tables.order('table_name ASC')
      clusters = @current_zookeeper.clusters
      render :json => {:clusters=>clusters, :tables=>tables}, :methods => [:has_queried_recently?]
    end
end
