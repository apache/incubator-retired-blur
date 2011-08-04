class Search < ActiveRecord::Base
  belongs_to :blur_table
  belongs_to :user

  def blur_query
    Blur::BlurQuery.new :queryStr     => self.query,
                        :fetch        => self.fetch,
                        :start        => self.offset,
                        :uuid         => Time.now.to_i*1000 + rand(1000),
                        :superQueryOn => self.super_query?,
                        :selector     => self.selector,
                        :userId       => User.find(self.user_id).username
  end

  def columns=(columns)
    write_attribute :columns, columns.to_json
  end
  def raw_columns
    # the column data passed back from the form
    JSON.parse(read_attribute(:columns))
  end
  def column_families
    # complete column families
    self.raw_columns.collect{|value| value.split('_')[1] if value.starts_with?('family')}.compact
  end
  def columns
    # hash with key = column_family and value = array of columns
    # just columns without column families, and with 'recordId' added in
    families = self.column_families
    columns = {}
    self.raw_columns.each do |raw_column|
      parts = raw_column.split('_')
      if parts[0] == 'column' and !families.include?(parts[1])
        columns[parts[1]] ||= ['recordId']
        columns[parts[1]] << parts[2]
      end
    end
    columns
  end
  def selector
    Blur::Selector.new :columnFamiliesToFetch => self.column_families,
                       :columnsToFetch        => self.columns
  end
  def fetch_results(table_name)
    BlurThriftClient.client.query(table_name, self.blur_query)
  end
end
