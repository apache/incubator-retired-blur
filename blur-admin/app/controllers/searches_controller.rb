class SearchesController < ApplicationController
  load_and_authorize_resource :only => [:load]

  before_filter :zookeepers, :only => :index
  before_filter :clean_column_data, :only => [:save, :update]

  JSON_RESPONSES = [:filters, :load, :update]
  respond_to :html, :except => JSON_RESPONSES
  respond_to :json, :only => JSON_RESPONSES

  #Show action that sets instance variables used to build the filter column
  def index
    # the .all call executes the SQL fetch, otherwise there are many more SQL fetches
    # required because of the lazy loading (in this case where a few more variables
    # depend on the result)
    @blur_tables = current_zookeeper.blur_tables.where('status = 4').order("table_name").includes(:cluster).all
    @blur_table = BlurTable.find_by_id(params[:table_id])
    if @blur_table.nil?
      @blur_table = @blur_tables[0]
      @query = ''
    else
      @query = params[:query].nil? ? '' : params[:query]
    end
    @columns = @blur_table.schema &preference_sort(current_user.column_preference.value || []) if @blur_table
    @searches = current_user.searches.order("name")
    @filter_table_collection = {}
    @blur_tables.each do |table|
        @filter_table_collection[table.cluster.name] ||= []
        @filter_table_collection[table.cluster.name] << [table.table_name, table.id]
    end
    respond_with
  end

  #Filter action to help build the tree for column families
  def filters
    blur_table = BlurTable.find params[:blur_table]
    columns = blur_table ? (blur_table.schema &preference_sort(current_user.column_preference.value || [])) : []
    filter_children = columns.collect do |family|
      col_fam = {:title => family['name'], :key => "family_-sep-_#{family['name']}", :addClass => 'check_filter', :select => true}
      col_fam[:children] = family['columns'].collect do |column|
        {:title => column['name'], :key => "column_-sep-_#{family['name']}_-sep-_#{column['name']}", :addClass=>'check_filter', :select => true}
      end
      col_fam
    end
    filter_list = { :title => 'All Families', :key => "neighborhood", :addClass => 'check_filter', :select => true, :children => filter_children}
    respond_with(filter_list)
  end

  #Create action is a large action that handles all of the filter data
  #and either saves the data or performs a search
  def create
    params[:column_data].delete( "neighborhood") if params[:column_data]
    search = Search.new(  :super_query      => params[:search] == '0',
                          :record_only      => params[:search] == '1' && params[:return] == '1',
                          :fetch            => params[:result_count].to_i,
                          :offset           => params[:offset].to_i,
                          :user_id          => current_user.id,
                          :query            => params[:query_string],
                          :blur_table_id    => params[:blur_table])
    search.column_object = params[:column_data]

    #use the model to begin building the blurquery
    blur_table = BlurTable.find params[:blur_table]

    blur_results = search.fetch_results(blur_table.table_name, current_zookeeper.blur_urls)

    # parse up the response object and reformat it to be @results.  @results holds the data
    # that will be passed to the view. @results is an array of results. Each result is a series
    # of nested hashes/arrays:
    #   result = {:id, :max_record_count, :column_families => column_families}
    #   column_families = {:column_family_name => [record]}
    #   record = {:recordId => recordId, :column_name => value}

    @result_count = blur_results.totalResults
    @result_time = -1 #blur_results.realTime
    @results = []
    blur_results.results.each do |blur_result_container|

      # drill down through the result object cruft to get the real result
      if !search.record_only
        blur_result = blur_result_container.fetchResult.rowResult.row
        records = blur_result.records
        id = blur_result.id
      else
        blur_result = blur_result_container.fetchResult.recordResult
        records = [blur_result.record]
        id = blur_result.rowid
      end

      # continue to next result if there is no returned data
      next if records.empty?

      max_record_count = records.
        collect {|record| record.family}.
        reduce(Hash.new(0)) {|count, family| count[family] += 1; count}.
        values.max

      result = {:max_record_count => max_record_count, :id => id}

      records.each do |blur_record|
        column_family = blur_record.family
        unless column_family.nil? # to compensate for a bug in blur that returns empty records if a column family is not selected
          record = {'recordId' => blur_record.recordId}
          blur_record.columns.each do |blur_column|
            record[blur_column.name] = blur_column.value
          end
          result[column_family] = [] unless result[column_family]
          result[column_family] << record
        end
      end
      @results << result
    end
    pref_sort = preference_sort(current_user.column_preference.value || [])
    @schema = Hash[search.schema(blur_table).sort &pref_sort]
    respond_with do |format|
      format.html { render 'create', :layout => false }
    end
  end

  #save action that loads the state of a saved action and returns a json to be used to populate the form
  def load
    respond_with(@search) do |format|
      format.json { render :json => @search, :methods => :column_object }
    end
  end

  #Delete action used for deleting a saved search from a user's saved searches
  def delete
    Search.find(params[:id]).delete
    @searches = current_user.searches.reverse
    respond_with(@searches) do |format|
      format.html { render :partial => "saved" }
    end
  end

  def save
    search = Search.new(:name             => params[:save_name],
                        :super_query      => params[:search] == '0',
                        :record_only      => params[:search] == '1' && params[:return] == '1',
                        :fetch            => params[:result_count].to_i,
                        :offset           => params[:offset].to_i,
                        :user_id          => current_user.id,
                        :query            => params[:query_string],
                        :blur_table_id    => params[:blur_table])
    search.column_object = params[:column_data]
    search.save
    @searches = current_user.searches.reverse

    respond_with(@searches) do |format|
      format.html { render :partial =>"saved" }
    end
  end

  def update
    Search.update(params[:id],
                        :name               => params[:save_name],
                        :super_query        => params[:search] == '0',
                        :record_only        => params[:search] == '1' && params[:return] == '1',
                        :fetch              => params[:result_count].to_i,
                        :offset             => params[:offset].to_i,
                        :user_id            => current_user.id,
                        :query              => params[:query_string],
                        :column_object      => params[:column_data],
                        :blur_table_id      => params[:blur_table])
    respond_with do |format|
      format.json { render :nothing => true }
    end
  end
  private
    def preference_sort(preferred_columns)
      lambda do |a, b|
        if preferred_columns.include? a[0] and !preferred_columns.include? b[0]
          -1
        elsif preferred_columns.include? b[0] and !preferred_columns.include? a[0]
          1
        else
          preferred_columns.index(a[0]) <=> preferred_columns.index(b[0])
        end
      end
    end

    def clean_column_data
      params[:column_data].delete 'neighborhood'
    end
end
