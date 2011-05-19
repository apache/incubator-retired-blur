class QueriesController < ApplicationController

	def show
	end


	def new
		client = setup_thrift
	
		bq = BG::BlurQuery.new
    bq.queryStr = params[:q]
		bq.fetch = 1
		table = 'employee_super_mart'
		blur_results = client.query(table, bq)
      
		@results = []
		blur_results.results.each do |result|
			location_id = result.locationId
			
			sel = BG::Selector.new
			sel.locationId = location_id
			fetched_row = client.fetchRow(table, sel)

			@results << JSON.pretty_generate(JSON.parse(fetched_row.as_json.to_json))
		end

		render :show
	end
end
