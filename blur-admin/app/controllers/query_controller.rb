class QueryController < ApplicationController

	def show
	end


	def new
		client = setup_thrift
	
		bq = BG::BlurQuery.new
    bq.queryStr = params[:q]
		bq.fetch = 25
		table = 'employee_super_mart'
		blur_results = client.query(table, bq)

		cfs = client.schema(table).columnFamilies
    @column_families = client.schema(table).columnFamilies.keys
		params[:column_data] ||= [@column_families.first]
		@visible_columns = cfs.reject { |k,v| !params[:column_data].include? k }
		@results = []
		@result_count = blur_results.totalResults

		blur_results.results.each do |result|
			location_id = result.locationId
			sel = BG::Selector.new
			sel.locationId = location_id
			row = client.fetchRow(table, sel).rowResult.row	
			
			# remove families not displayed
			visible = row.columnFamilies.find_all { |cf| @visible_columns.keys.include? cf.family }
			# find column family with most records to pad the rest for the table
      max_record_count = visible.collect {|cf| cf.records.keys.count }.max
     
		  # organize into multidimensional array of rows and columns
			table_rows = Array.new(max_record_count) { [] }
			(0...max_record_count).each do |i|

				@visible_columns.each do |columnFamilyName, set|
					columnFamily = row.columnFamilies.find { |cf| cf.family == columnFamilyName }
					table_rows[i] << cfspan = []

          if columnFamily
						count = columnFamily.records.values.count
						if i < count
							set.each do |s|
								found_set = columnFamily.records.values[i].find { |col| s == col.name }
								cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
							end
						else						
							set.count.times { |t| cfspan << ' ' }
						end
					else
						set.count.times { |t| cfspan << ' ' }
					end
				end
			end

			record = {:id => row.id, :max_record_count => max_record_count, :row => row, :table_rows => table_rows}
			
			@results << record
		end


		close_thrift

		render :show
	end

  def cancel
    client = setup_thrift
    client.cancelQuery(param[:table], param[:uuid])
  end


  def current_queries
    client = setup_thrift
    running_queries = client.currentQueries(params[:table])
    close_thrift

    render :json => running_queries
  end
end
