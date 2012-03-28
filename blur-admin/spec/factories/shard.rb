FactoryGirl.define do
  factory :shard do
    blur_version              { "1.#{rand 10}.#{rand 10}" }
    sequence (:node_name)     { |n| "Test Node ##{n}" }
    status                    { rand 3 }
  end
end