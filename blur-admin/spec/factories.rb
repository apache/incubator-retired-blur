# Determines how many models are created in a has_many relationship when
# using a 'plural' builder, i.e. Factory.build(:zookeeper_with_controllers)
# As the name suggests the number of models grows with Theta = a^(b-1) where
# b is model association chain length and a is @recursive_factor
@recursive_factor = 3

# Basic model definitions

Factory.define :zookeeper do |t|
  t.sequence (:name) { |n| "Test Zookeeper ##{n}" }
  t.sequence (:url)  { |n| "zookeeper#{n}.blur.example.com" }
  t.status           { rand 2 }
end

Factory.define :controller do |t|
  t.sequence (:node_name)     { |n| "Test Node ##{n}" }
  t.sequence (:node_location) { |n| "node#{n}.blur.example.com" }
  t.status                    { rand 2 }
  t.blur_version              { "1.#{rand 10}.#{rand 10}" }
end

Factory.define :cluster do |t|
  t.sequence (:name) { |n| "Test Cluster ##{n}" }
end

Factory.define :shard do |t|
  t.blur_version              { "1.#{rand 10}.#{rand 10}" }
  t.sequence (:node_name)     { |n| "Test Node ##{n}" }
  t.sequence (:node_location) { |n| "node#{n}.blur.example.com" }
  t.status                    { rand 2 }
end

Factory.define :blur_table do |t|
  t.sequence (:table_name) { |n| "Test Blur Table ##{n}" }
  t.current_size           { 10**12 + rand(999 * 10 ** 12) } #Between a terrabyte and a petabyte
  t.query_usage            { rand 500 }                      #Queries per second
  t.record_count           { 10**6 + rand(999 * 10 ** 6) }   #Between a million and a billion 
  t.status                 { 1 + rand(2) }
  t.sequence (:table_uri)  { |n| "blur_table#{n}.blur.example.com" }
  t.table_analyzer 'standard.table_analyzer'
  t.table_schema { |blur_table| "{\"table\":\"#{blur_table.table_name}\",\"setTable\":true,\"setColumnFamilies\":true,\"columnFamiliesSize\":2,\"columnFamilies\":{\"Column Family #1\":[\"Column #1\",\"Column #2\",\"Column #3\"],\"Column Family #1\":[\"Column #1\",\"Column #2\",\"Column #3\"]}}" }
end

Factory.define :blur_query do |t|
  t.sequence (:query_string) { |n| "Blur Query ##{n} Query String" }
  t.cpu_time  { rand 10 * 10 ** 3 } #Between 0 and 10 seconds
  t.real_time { |blur_query| blur_query.cpu_time + rand( 10 * 10 ** 3) } #Between 0 and 10 additional seconds
  t.complete  { rand 2 }
  t.interrupted { rand(5) == 0 } # 20% chance
  t.running     { rand(5) == 0 } # 20% chance
  t.uuid        { rand 10 ** 8 }
  t.super_query_on { rand(4) != 0 } # 75% chance
  t.start { rand 10 ** 6 }
  t.fetch_num { rand 10 ** 6 }
  t.userid { "Test User ##{rand 20}" }
  #t.selector_column_families
  #t.selector_columns
  #t.pre_filters
  #t.post_filters
end

# Create models with association chains already created. These real objects and
# persist them in the database

Factory.define :zookeeper_with_cluster, :parent => :zookeeper do |t|
  t.after_create do |zookeeper|
    Factory.create :controller, :zookeeper => zookeeper
    Factory.create :cluster,    :zookeeper => zookeeper
  end
end

Factory.define :zookeeper_with_clusters, :parent => :zookeeper do |t|
  t.after_create do |zookeeper|
    @recursive_factor.times { Factory.create :controller, :zookeeper => zookeeper }
    @recursive_factor.times { Factory.create :cluster,    :zookeeper => zookeeper }
  end
end

Factory.define :zookeeper_with_shard, :parent => :zookeeper_with_cluster  do |t|
  t.after_create do |zookeeper|
    zookeeper.clusters.each { |cluster| Factory.create(:shard, :cluster => cluster) }
  end
end

Factory.define :zookeeper_with_shards, :parent => :zookeeper_with_clusters  do |t|
  t.after_create do |zookeeper|
    zookeeper.clusters.each { |cluster| @recursive_factor.times {Factory.create(:shard, :cluster => cluster)} }
  end
end

Factory.define :zookeeper_with_blur_table, :parent => :zookeeper_with_shard  do |t|
  t.after_create do |zookeeper|
    zookeeper.shards.each { |shard| Factory.create(:blur_table, :shards => [shard]) }
  end
end

Factory.define :zookeeper_with_blur_tables, :parent => :zookeeper_with_shards  do |t|
  t.after_create do |zookeeper|
    # NOTICE: this code does not ensure that every shard will have a blur table,
    # only that each blur table will have a shard
    (zookeeper.shards.count * @recursive_factor).times do
      blur_table = Factory.create :blur_table
      # add an association to each shard with 50% probability
      zookeeper.shards.count.times { |n| blur_table.shards << zookeeper.shards[n] if rand(2) == 1 }
      # if no associations were added, then add a random one
      blur_table.shards << zookeeper.shards[rand(zookeeper.shards.count)] if blur_table.shards.empty?
    end
  end
end

Factory.define :zookeeper_with_blur_query, :parent => :zookeeper_with_blur_table  do |t|
  t.after_create do |zookeeper|
    zookeeper.blur_tables.each { |blur_table| Factory.create(:blur_query, :blur_table => blur_table) }
  end
end

Factory.define :zookeeper_with_blur_queries, :parent => :zookeeper_with_blur_tables  do |t|
  t.after_create do |zookeeper|
    zookeeper.blur_tables.each { |blur_table| @recursive_factor.times {Factory.create(:blur_query, :blur_table => blur_table)} }
  end
end

Factory.define :blur_table_with_blur_query, :parent => :blur_table do |t|
  t.after_create { |blur_table| Factory.create(:blur_query, :blur_table => blur_table)} 
end

Factory.define :blur_table_with_blur_queries, :parent => :blur_table do |t|
  t.after_create { |blur_table| @recursive_factor.times {Factory.create(:blur_query, :blur_table => blur_table)} } 
end
