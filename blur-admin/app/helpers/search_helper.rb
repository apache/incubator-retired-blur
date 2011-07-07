module SearchHelper
  def is_valid_search?(search)
    #get the table schema for the current table
    schema = @blur_tables.table_schema["columnFamilies"]

    #get the array of the checked columns from the search
    raw_column = JSON.parse search.columns

    search_columns = {}

    #get all of the
    raw_column.each do |value|
      parts = value.split('_')
      if parts[0] == 'column'
        search_columns[parts[1]] << parts[2]
      end
    end

    #TODO logic for set difference
    schema.each_with_index do |family, children|
      test_arr = search_columns[family] - children
      if test_arr.length > 0
        return false
      end
    end

    #if every test above passes than this is valid
    true
  end
end
