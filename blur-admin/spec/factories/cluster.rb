FactoryGirl.define do
  factory :cluster do
    sequence (:name) { |n| "Test Cluster ##{n}" }
    safe_mode        false
    zookeeper_id     1
    can_update       false
  end
end
