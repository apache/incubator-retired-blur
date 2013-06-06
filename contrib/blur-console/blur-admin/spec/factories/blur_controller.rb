FactoryGirl.define do
  factory :blur_controller do
    sequence (:node_name)     { |n| "Test Node ##{n}" }
    controller_status                    { rand 3 }
    blur_version              { "1.#{rand 10}.#{rand 10}" }
  end
end