class QueryController < ApplicationController

	def show
	  client = setup_thrift
	  
	  @tables = client.tableList
	  @columns = client.schema(@tables.first) unless @tables.blank?
	  close_thrift
	end
	
	def filters
	  client = setup_thrift
	  table = params[:table]
	  @columns = client.schema(table)
	  close_thrift
  end

	def create
		client = setup_thrift
		
		# TODO: Change this when the new Blur is deployed to use the selectors on search
		# TODO: Add in fetch filter, paging, superQueryOn/Off
		
	  table = params[:t]
		bq = Blur::BlurQuery.new
    bq.queryStr = params[:q]
		bq.fetch = 25
		bq.uuid = Time.now.to_i*1000+rand(1000)

		blur_results = client.query(table, bq)

    families = params[:column_data].collect{|value|  value.split('_')[1] if value.starts_with?('family') }.compact
    columns = {}
    params[:column_data].each do |value|
      parts = value.split('_')
      if parts[0] == 'column' and !families.include?(parts[1])
        parts = value.split('_')
        if (!columns.has_key?(parts[1]))
          columns[parts[1]] = []
        end
        columns[parts[1]] << parts[2]
      end
    end
    
    families_with_columns = {}
    families.each do |family|
      families_with_columns[family] = client.schema(table).columnFamilies[family]
    end
    
    visible_families = (families + columns.keys).uniq

		#cfs = client.schema(table).columnFamilies
    #@column_families = client.schema(table).columnFamilies.keys
		#params[:column_data] ||= [@column_families.first]
		#@visible_columns = cfs.reject { |k,v| !params[:column_data].include? k }
		@results = []
		@result_count = blur_results.totalResults

		blur_results.results.each do |result|
			location_id = result.locationId
			sel = Blur::Selector.new
			sel.locationId = location_id
			sel.columnFamiliesToFetch = families unless families.blank?
			sel.columnsToFetch = columns unless columns.blank?
			row = client.fetchRow(table, sel).rowResult.row	
			
			# remove families not displayed
      # visible = row.columnFamilies.find_all { |cf| @visible_columns.keys.include? cf.family }
			# find column family with most records to pad the rest for the table
      max_record_count = row.columnFamilies.collect {|cf| cf.records.keys.count }.max
     
		  # organize into multidimensional array of rows and columns
			table_rows = Array.new(max_record_count) { [] }
			(0...max_record_count).each do |i|
				visible_families.each do |columnFamilyName|
					columnFamily = row.columnFamilies.find { |cf| cf.family == columnFamilyName }
					table_rows[i] << cfspan = []

          if columnFamily
						count = columnFamily.records.values.count
						if i < count
						  if families.include? columnFamilyName
						    families_with_columns[columnFamilyName].each do |s|
						      found_set = columnFamily.records.values[i].find { |col| s == col.name }
  								cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
					      end
					    else
					      columns[columnFamilyName].each do |s|
					        found_set = columnFamily.records.values[i].find { |col| s == col.name }
  								cfspan << (found_set.nil? ? ' ' : found_set.values.join(', '))
				        end
				      end
						else		
						  if families.include? columnFamilyName				
							  families_with_columns[columnFamilyName].count.times { |t| cfspan << ' ' }
						  else
						    columns[columnFamilyName].count.times { |t| cfspan << ' ' }
					    end
						end
					else
						if families.include? columnFamilyName				
						  families_with_columns[columnFamilyName].count.times { |t| cfspan << ' ' }
					  else
					    columns[columnFamilyName].count.times { |t| cfspan << ' ' }
				    end
					end
				end
			end

			record = {:id => row.id, :max_record_count => max_record_count, :row => row, :table_rows => table_rows}
			
			@results << record
		end
		close_thrift
		
		@all_columns = families_with_columns.merge columns
		
		render :template=>'query/create.html.haml', :layout => false
	end

end
