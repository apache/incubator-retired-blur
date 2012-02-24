FactoryGirl.define do
  factory :controller do
    sequence (:node_name)     { |n| "Test Node ##{n}" }
    status                    { rand 3 }
    blur_version              { "1.#{rand 10}.#{rand 10}" }
  end
end