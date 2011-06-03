module DataHelper
  def get_shards(table)
    hosts = {}
    @tserver[table].each do |shard,host|
      hosts[host] = [] unless hosts.has_key? host
      hosts[host] << shard
    end
    hosts
  end

  def get_count(table)
		number_with_delimiter(@tcount[table], :delimiter => ',')
  end

  def get_a_blur_table(table)
    BlurTables.where(:table_name => table).first
  end

  def get_loc(table)
    @tdesc[table].tableUri
  end

end
