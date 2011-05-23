module DataHelper
  def get_shards(table)
    shards = @tdesc[table].shardNames
    shards_str = ''
    shards.each do |b|
      shards_str += b + ', '
    return shards_str
    end
  end

  def get_count(table)
    #get the count
    return 'count ###'
  end

  def get_size(table)
    #get the size
    return 'size ###'
  end

  def get_enable(table)
    if @tdesc[table].isEnabled
      return 'enabled'
    end
    return 'disabled'
  end

  def get_loc(table)
    return @tdesc[table].tableUri
  end

  def get_defin(table)
    return "a definition"
  end

end