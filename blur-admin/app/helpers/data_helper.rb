module DataHelper
  def get_shards(table)
    hosts = Hash.new
    @tserver[table].each do |shard,host|
      if hosts.has_key?(host)
        hosts[host].push(shard)
      else
        hosts[host] = [shard]
      end
    end
    return hosts
  end

  def get_count(table)
		return number_with_delimiter(@tcount[table], :delimiter => ',')
  end

  def get_a_blur_table(table)
    return BlurTables.where(:table_name => table).first
  end

  def get_loc(table)
    return @tdesc[table].tableUri
  end

end
