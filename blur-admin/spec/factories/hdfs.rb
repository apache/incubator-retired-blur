FactoryGirl.define do
  factory :hdfs do
    host { "nic-factory-hdfs.com" }
    port { "9000" }
    name { "factory_hdfs" }
  end
end