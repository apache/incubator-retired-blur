# Determines how many models are created in a has_many relationship when
# using a 'plural' builder, i.e. Factory.build(:zookeeper_with_controllers)
# Be careful setting this to a large number when creating a long chained model; as the name
# suggests the number of models grows with Theta = a^(b-1) where b is model association chain
# length and a is @recursive factor
@recursive_factor = 3

# Basic model definitions

Factory.define :zookeeper do |t|


end

Factory.define :controller do |t| end

Factory.define :cluster do |t| end

Factory.define :shard do |t| end

Factory.define :blur_table do |t| end

Factory.define :blur_query do |t| end

# More fully defined models

Factory.define :blur_table_with_schema, :parent => :blur_table do |t|
  t.table_name { "Blur Table ##{Factory.next :blur_table_name}" }
  t.table_schema {|blur_table| "{\"table\":\"#{blur_table.table_name}\",\"setTable\":true,\"setColumnFamilies\":true,\"columnFamiliesSize\":2,\"columnFamilies\":{\"Column Family #1\":[\"Column #1\",\"Column #2\",\"Column #3\"],\"Column Family #1\":[\"Column #1\",\"Column #2\",\"Column #3\"]}}" }
  
end


# Create models with association chains already created. These  create create real objects and
# persist them in the database
Factory.define :zookeeper_with_controller, :parent => :zookeeper  do |t|
  t.after_create { |zookeeper| Factory.create(:controller, :zookeeper => zookeeper) }
end

Factory.define :zookeeper_with_controllers, :parent => :zookeeper do |t|
  t.after_create { |zookeeper| @recursive_factor.times {Factory.create(:controller, :zookeeper => zookeeper)} }
end

Factory.define :zookeeper_with_cluster, :parent => :zookeeper_with_controller  do |t|
  t.after_create do |zookeeper|
    zookeeper.controllers.each { |controller| Factory.create(:cluster, :controller => controller) }
  end
end

Factory.define :zookeeper_with_clusters, :parent => :zookeeper_with_controllers  do |t|
  t.after_create do |zookeeper|
    zookeeper.controllers.each { |controller| @recursive_factor.times {Factory.create(:cluster, :controller => controller)} }
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

Factory.define :blur_table_with_blur_query, :parent => :blur_table do |t|
  t.after_create { |blur_table| Factory.create(:blur_query, :blur_table => blur_table)} 
end

Factory.define :blur_table_with_blur_queries, :parent => :blur_table do |t|
  t.after_create { |blur_table| @recursive_factor.times {Factory.create(:blur_query, :blur_table => blur_table)} } 
end

# Sequences
Factory.sequence :blur_table_name do |n|
  n
end
