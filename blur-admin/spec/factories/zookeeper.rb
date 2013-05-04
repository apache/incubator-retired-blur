FactoryGirl.define do
  factory :zookeeper do
    sequence (:name)      { |n| "Test Zookeeper ##{n}" }
    sequence (:url)       { |n| "zookeeper#{n}.blur.example.com" }
    sequence (:blur_urls) { |n| "host#{n}:40010"}
    online_ensemble_nodes { "[\"nic-blurtop.nearinfinity.com\"]" }
    zookeeper_status                { rand 2 }

    ignore do
      recursive_factor 3
    end

    factory :zookeeper_with_cluster do
      after_create do |zookeeper|
        FactoryGirl.create_list(:blur_controller, 1, :zookeeper => zookeeper)
        FactoryGirl.create_list(:cluster, 1, :zookeeper => zookeeper)
      end
    end

    factory :zookeeper_with_clusters do
      after_create do |zookeeper, evaluator|
        FactoryGirl.create_list(:blur_controller, evaluator.recursive_factor, :zookeeper => zookeeper)
        FactoryGirl.create_list(:cluster, evaluator.recursive_factor, :zookeeper => zookeeper)
      end
    end

    factory :zookeeper_with_blur_table, :parent => :zookeeper_with_cluster do
      after_create do |zookeeper|
        zookeeper.clusters.each { |cluster| FactoryGirl.create(:blur_table, :cluster => cluster) }
        zookeeper.clusters.each { |cluster| FactoryGirl.create(:blur_shard, :cluster => cluster) }
      end
    end

    factory :zookeeper_with_blur_tables, :parent => :zookeeper_with_cluster do
      after_create do |zookeeper, evaluator|
        zookeeper.clusters.each { |cluster| FactoryGirl.create_list(:blur_table, evaluator.recursive_factor, :cluster => cluster) }
        zookeeper.clusters.each { |cluster| FactoryGirl.create_list(:blur_shard, evaluator.recursive_factor, :cluster => cluster) }
      end
    end

    factory :zookeeper_with_blur_query, :parent => :zookeeper_with_blur_table do
      after_create do |zookeeper|
        zookeeper.blur_tables.each { |blur_table| FactoryGirl.create(:blur_query, :blur_table => blur_table) }
      end
    end

    factory :zookeeper_with_blur_queries, :parent => :zookeeper_with_blur_tables do
      after_create do |zookeeper, evaluator|
        zookeeper.blur_tables.each { |blur_table| FactoryGirl.create_list(:blur_query, evaluator.recursive_factor, :blur_table => blur_table) }
      end
    end
  end
end