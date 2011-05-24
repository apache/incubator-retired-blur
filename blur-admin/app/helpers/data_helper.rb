module DataHelper
  def get_shards(table)
    shards_str = ""
    @tserver[table].each do |shard,host|
      shards_str += shard + "->" + host + "  "
    end
    return shards_str
  end

  def get_count(table)
		return number_with_delimiter(@tcount[table], :delimiter => ',')
  end

  def get_size(table)
    #get the size
    return 'size ###'
  end

  def get_loc(table)
    return @tdesc[table].tableUri
  end

  def get_defin(table)
    return "a definition"
  end

end