FactoryGirl.define do
  factory :cluster do
    sequence (:name) { |n| "Test Cluster ##{n}" }
  end
end