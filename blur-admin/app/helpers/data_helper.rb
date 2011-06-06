module DataHelper

  def shards(table)
    hosts = {}
    @tserver[table].each do |shard,host|
      hosts[host] = [] unless hosts.has_key? host
      hosts[host] << shard
    end
    hosts
  end

end
