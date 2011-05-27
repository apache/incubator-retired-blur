module RuntimeHelper
  def mysql_queries()
    @running_queries = BlurQueries.where(:table_name => 'employee_super_mart').all
  end
end