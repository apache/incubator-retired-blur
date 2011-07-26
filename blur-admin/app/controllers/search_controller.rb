class SearchController < ApplicationController
  before_filter :current_zookeeper, :only => :show
  before_filter :zookeepers, :only => :show

	#Show action that sets instance variables used to build the filter column
  def show
    # the .all call executes the SQL fetch, otherwise there are many more SQL fetches
    # required because of the lazy loading (in this case where a few more variables 
    # depend on the result)
    @blur_tables = @current_zookeeper.blur_tables.order("table_name").all
    @blur_table = @blur_tables.first
	  @columns = @blur_table.schema &preference_sort if @blur_table
    @searches = current_user.searches.order("name")
	end

	#Filter action to help build the tree for column families
  def filters
    @blur_table = BlurTable.find params[:blur_table_id]
    begin
      @columns = @blur_table.schema &preference_sort
    rescue NoMethodError
      @columns = []
    end
    #TODO render the new saved list
	  render :partial => 'filters'
  end

	#Create action is a large action that handles all of the filter data
  #and either saves the data or performs a search
  def create
    #if the search_id param is set than the user is trying to directly run a saved query
    if params[:search_id]
      search = Search.find params[:search_id]
    #else build a new search to be used for this specific search
    else
      params[:column_data].delete( "neighborhood")
      search = Search.new(:blur_table_id => params[:blur_table],
                          :super_query   =>!params[:super_query].nil?,
                          :columns       => params[:column_data],
                          :fetch         => params[:result_count].to_i,
                          :offset        => params[:offset].to_i,
                          :user_id       => @current_user.id,
                          :query         => params[:query_string])
    end

    #use the model to begin building the blurquery
    blur_table = BlurTable.find params[:blur_table]

    # create a schema hash which contains the column_family => columns which the search is over
    # initialize to be set of incomplete column families
    @schema = search.columns
    # add complete column families / columns
    search.column_families.each do |family|
      @schema[family] = ['recordId']
      @schema[family] << blur_table.schema[family]
      @schema[family].flatten!
    end

    # sort column families by user preferences, then by alphabetical order
    @schema = Hash[@schema.sort &preference_sort]

    blur_results = search.fetch_results(blur_table.table_name)

    # parse up the response object from blur and prepares it as a table
    # Definitions:
    #   Result: consists  of one or more rows.  Each result has an
    #           identifying rowId (I know, confusing...)
    #   Row: A row consisting of one record for each column family it spans.

    @results = []
    @result_count = blur_results.totalResults
    @result_time = blur_results.realTime
    blur_results.results.each do |result_container|
      # drill down through the result object cruft to get the real result
      result = result_container.fetchResult.rowResult.row 
      # number of rows the result will span
      row_count = result.columnFamilies.collect {|cf| cf.records.keys.count }.max

      # organize into multidimensional array of rows and columns
      rows = Array.new(row_count) { [] }
      (0...row_count).each do |row| # for each row in the result, row here is really just the row number
        @schema.keys.each do |column_family_name| # for each column family in the row
          column_family = result.columnFamilies.find { |cf| cf.family == column_family_name }
          rows[row] << cfspan = []

          if column_family and row < column_family.records.values.count
            @schema[column_family_name].each do |column|
              found_set = column_family.records.values[row].find { |col| column == col.name }
              if !(column == 'recordId')
                cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
              else
                cfspan << column_family.records.keys[row]
              end
            end
          else # otherwise pad with blank space
            @schema[column_family_name].count.times { |count_time| cfspan << ' ' }
          end
        end
      end

      result_rows  = {:id => result.id, :row_count => row_count, :rows => rows}

      @results << result_rows
    end

    #reorder the CFs to use the preference
    @family_order = (current_user.saved_cols & @schema.keys) | @schema.keys

    render :template=>'search/create.html.haml', :layout => false
  end

  #save action that loads the state of a saved action and returns a json to be used to populate the form
  def load
    #TODO logic to check if the saved search is valid if it is render the changes to the page
    #otherwise change the state of the save and load what you can
    @search = Search.find params['search_id']
    search = JSON.parse @search.to_json
    search["search"]["columns"] = @search.raw_columns
    render :json => {:saved => search, :success => true }
  end

  #Delete action used for deleting a saved search from a user's saved searches
  def delete
    Search.find(params[:search_id]).delete
    @searches = current_user.searches.reverse
    @blur_table = BlurTable.find params[:blur_table]
    respond_to do |format|
      format.html {render :partial =>"saved" }
    end
  end

  def reload
    @searches = current_user.searches.reverse
    @blur_table = BlurTable.find params[:blur_table]
    respond_to do |format|
      format.html {render :partial =>"saved"}
    end
  end
  
  def save
    drop = params[:column_data].first == "neighborhood_all"? params[:column_data].drop(1) : params[:column_data]
    Search.create(:name          => params[:save_name],
                  :blur_table_id => params[:blur_table],
                  :super_query   => params[:super_query],
                  :columns       => drop,
                  :fetch         => params[:result_count].to_i,
                  :offset        => params[:offset].to_i,
                  :user_id       => current_user.id,
                  :query         => params[:query_string])
    @searches = current_user.searches.reverse
    @blur_table = BlurTable.find params[:blur_table]

    respond_to do |format|
      format.html {render :partial =>"saved"}
    end
  end

  private
    def preference_sort
      lambda do |a, b|
        if @current_user.saved_cols.include? a[0] and !@current_user.saved_cols.include? b[0]
          -1
        elsif @current_user.saved_cols.include? b[0] and !@current_user.saved_cols.include? a[0]
          1
        else
          a[0] <=> b[0]
        end
      end
    end
end
