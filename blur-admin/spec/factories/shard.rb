FactoryGirl.define do
  factory :shard do
    blur_version              { "1.#{rand 10}.#{rand 10}" }
    sequence (:node_name)     { |n| "Test Node ##{n}" }
    status                    { rand 3 }
    
    factory :shard_with_cluster do
      after_create do |shard|
        FactoryGirl.create_list(:cluster, 1, :shards => [shard])
      end
    end
  end
end