FactoryGirl.define do
  factory :hdfs do
    host { "nic-factory-hdfs.com" }
    port { "9000" }
    name { "factory_hdfs" }

    ignore do
      recursive_factor 3
    end

    factory :hdfs_with_stats do
      after_create do |hdfs, evaluator|
        FactoryGirl.create_list(:hdfs_stat, evaluator.recursive_factor, :hdfs => hdfs)
      end
    end
  end
end