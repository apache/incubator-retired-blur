# Read about factories at https://github.com/thoughtbot/factory_girl

FactoryGirl.define do
  factory :admin_setting do
    setting 'regex'
    value   '.*'
  end
end