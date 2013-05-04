class BlurTablesController < ApplicationController
  load_and_authorize_resource :shallow => true, :only => [:comment, :terms, :hosts, :schema]

  before_filter :zookeepers, :only => :index

  respond_to :html, :only => [:index]
  respond_to :json

  def index
    @clusters = current_zookeeper.clusters_with_query_status(current_user)
    respond_with(@clusters) do |format|
      format.json { render :json => @clusters, :methods => [:blur_tables], :blur_tables => true }
    end
  end

  def enable
    table_update_action do |table|
      table.table_status = STATUS[:enabling]
      table.enable current_zookeeper.blur_urls
      table.save
      Audit.log_event(current_user, "Table, #{table.table_name}, was enabled",
                      "blur_table", "update", current_zookeeper)
    end
  end

  def disable
    table_update_action do |table|
      table.table_status = STATUS[:disabling]
      table.disable current_zookeeper.blur_urls
      table.save
      Audit.log_event(current_user, "Table, #{table.table_name}, was disabled",
                      "blur_table", "update", current_zookeeper)
    end
  end

  def destroy
    destroy_index = params[:delete_index] == 'true' # Destroy underlying index boolean
    tables = params[:tables]                        # Tables being destroyed
    blur_urls = current_zookeeper.blur_urls         # Cached blur_urls

    BlurTable.find(tables).each do |table|
      table.blur_destroy destroy_index, blur_urls
      index_message = "and underlying index " if destroy_index
      Audit.log_event(current_user, "Table, #{table.table_name}, #{index_message}was deleted",
                      "blur_table", "update", current_zookeeper)
    end
    BlurTable.destroy tables

    respond_to do |format|
      format.json { render :json => {} }
    end
  end

  def terms
    terms = @blur_table.terms current_zookeeper.blur_urls, params[:family], params[:column], params[:startwith], params[:size].to_i

    respond_with(terms)
  end

  def hosts
    respond_with(@blur_table) do |format|
      format.json { render :json => @blur_table.hosts }
    end
  end

  def schema
    respond_with(@blur_table) do |format|
      format.json { render :json => @blur_table.schema }
    end
  end

  def comment
    raise "No comment provided!" if params[:comment].nil?
    @blur_table.comments = params[:comment]
    @blur_table.save

    respond_with(@table)
  end

  private

  STATUS = {:enabling => 5, :active => 4, :disabling => 3, :disabled => 2, :deleting => 1, :deleted => 0}
  STATUS_SELECTOR = {:active => [4, 3], :disabled => [2, 5, 1], :deleted => [0]}

  def table_update_action
    BlurTable.find(params[:tables]).each do |table|
      yield table
    end

    respond_to do |format|
      format.json { render :json => {} }
    end
  end
end
