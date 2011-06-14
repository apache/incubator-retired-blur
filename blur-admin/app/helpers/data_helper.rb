module DataHelper

  def shards(table)
    hosts = {}
    table.server.each do |shard,host|
      hosts[host] = [] unless hosts.has_key? host
      hosts[host] << shard
    end
    hosts
  end

end
