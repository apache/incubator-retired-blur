class SearchController < ApplicationController
  before_filter :current_zookeeper, :only => :show
  before_filter :zookeepers, :only => :show

  #Show action that sets instance variables used to build the filter column
  def show
    # the .all call executes the SQL fetch, otherwise there are many more SQL fetches
    # required because of the lazy loading (in this case where a few more variables 
    # depend on the result)
    @blur_tables = @current_zookeeper.blur_tables.order("table_name").all
    @blur_table = @blur_tables[0]
	  @columns = @blur_table.schema &preference_sort if @blur_table
    @searches = current_user.searches.order("name")
	end

	#Filter action to help build the tree for column families
  def filters
    @blur_table = BlurTable.find params[:blur_table_id]
    @columns = (@blur_table ? (@blur_table.schema &preference_sort) : [])
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
      search = Search.new(:super_query  =>!params[:super_query].nil?,
                          :columns      => params[:column_data],
                          :fetch        => params[:result_count].to_i,
                          :offset       => params[:offset].to_i,
                          :user_id      => current_user.id,
                          :query        => params[:query_string])
    end

    #use the model to begin building the blurquery
    blur_table = BlurTable.find params[:blur_table]


    blur_results = search.fetch_results(blur_table.table_name)

    # parse up the response object and reformat it to be a @results
    # Results Object:
    #   @results is an array of results. Each result is a 3-dimension nested hash/array with the following:
    #     result = {:id, :max_record_count, :column_families => column_families}
    #     column_families = {:column_family => [record]}
    #     record = {:column => value, :recordId => recordId}

    @results = []
    blur_results.results.each do |blur_result_container|
      # drill down through the result object cruft to get the real result
      blur_result = blur_result_container.fetchResult.rowResult.row 
      # continue to next result if there is no returned data
      next if blur_result.columnFamilies.empty?

      max_record_count = blur_result.columnFamilies.collect {|cf| cf.records.keys.count }.max

      result = {:max_record_count => max_record_count, :id => blur_result.id}

      blur_result.columnFamilies.each do |blur_column_family|
        column_family = blur_column_family.family
        records = []
        blur_column_family.records.each do |record_id, blur_columns|
          record = {'recordId' => record_id}
          blur_columns.each do |blur_column|
            unless blur_column == 'recordId'
              column = blur_column.name
              record[column] = blur_column.values.join ', '
            end
          end
          records << record
        end
        result[column_family] = records
      end
      @results << result
    end

    # create a schema hash which contains the column_family => columns which the search is over
    # initialize to be set of incomplete column families
    @schema = search.columns
    # add complete column families / columns
    search.column_families.each do |family|
      @schema[family] = ['recordId']
      @schema[family] << blur_table.schema[family]
      @schema[family].flatten!
    end
    # finally, sort column families by user preferences, then by alphabetical order
    @schema = Hash[@schema.sort &preference_sort]

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
    params[:column_data].delete 'neighborhood'
    Search.create(:name         => params[:save_name],
                  :super_query  =>!params[:super_query].nil?,
                  :columns      => params[:column_data],
                  :fetch        => params[:result_count].to_i,
                  :offset       => params[:offset].to_i,
                  :user_id      => current_user.id,
                  :query        => params[:query_string])
    @searches = current_user.searches.reverse
    @blur_table = BlurTable.find params[:blur_table]

    respond_to do |format|
      format.html {render :partial =>"saved"}
    end
  end
  
  def update
    params[:column_data].delete 'neighborhood'
    search = Search.find params[:search_id]
    search.update_attributes(:name        => params[:save_name],
                             :super_query =>!params[:super_query].nil?,
                             :columns     => params[:column_data],
                             :fetch       => params[:result_count].to_i,
                             :offset      => params[:offset].to_i,
                             :user_id     => current_user.id,
                             :query       => params[:query_string])

    render :nothing => true
  end
  private
    def preference_sort
      lambda do |a, b|
        if current_user.saved_cols.include? a[0] and !current_user.saved_cols.include? b[0]
          -1
        elsif current_user.saved_cols.include? b[0] and !current_user.saved_cols.include? a[0]
          1
        else
          a[0] <=> b[0]
        end
      end
    end
end
