class Search < ActiveRecord::Base
  belongs_to :blur_table
  belongs_to :user
  
  before_save :marshal_columns
  
  attr_accessor :column_object

  def blur_query
    Blur::BlurQuery.new :simpleQuery  => Blur::SimpleQuery.new(:queryStr => query, :superQueryOn => super_query?),
                        :fetch        => fetch,
                        :start        => offset,
                        :uuid         => Time.now.to_i*1000 + rand(1000),
                        :selector     => selector,
                        :userContext  => User.find(user_id).username
  end
  
  def column_object
    unless @column_object
      @column_object = columns ? JSON.parse(columns) : []
    end
    @column_object
  end

  def column_families
    puts column_object.inspect
    column_object.collect{|value| value.split('_-sep-_')[1] if value.starts_with?('family')}.compact
  end
  
  def columns_hash
    # hash with key = column_family and value = array of columns
    # just columns without column families, and with 'recordId' added in
    families = column_families
    cols = {}
    column_object.each do |raw_column|
      parts = raw_column.split('_-sep-_')
      if parts[0] == 'column' and !families.include?(parts[1])
        cols[parts[1]] ||= ['recordId']
        cols[parts[1]] << parts[2]
      end
    end
    cols
  end
  
  def selector
    Blur::Selector.new :columnFamiliesToFetch => column_families,
                       :columnsToFetch        => columns_hash
  end
  def fetch_results(table_name, host, port)
    BlurThriftClient.client(host, port).query(table_name, blur_query)
  end

  def schema(blur_table)
    tmp_schema = columns_hash
    column_families.each do |family|
      tmp_schema[family] = ['recordId']
      tmp_schema[family] << blur_table.schema[family]
      tmp_schema[family].flatten!
    end
    puts "schema: #{tmp_schema}"
    tmp_schema
  end
  
  private
  def marshal_columns
    write_attribute(:columns, column_object.to_json) if column_object
  end
end
