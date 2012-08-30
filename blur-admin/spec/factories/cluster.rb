FactoryGirl.define do
  factory :cluster do
    sequence (:name) { |n| "Test Cluster ##{n}" }
    safe_mode        false
    zookeeper_id     1
    can_update       false

    ignore do
      recursive_factor 3
    end

    factory :cluster_with_shard do
      after_create do |cluster|
        FactoryGirl.create_list(:blur_shard, 1, :cluster => cluster)
      end
    end

    factory :cluster_with_shards do
      after_create do |cluster|
        FactoryGirl.create_list(:blur_shard, 3, :cluster => cluster)
      end
    end
  end
end
