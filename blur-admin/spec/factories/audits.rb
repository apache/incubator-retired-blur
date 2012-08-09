# Read about factories at https://github.com/thoughtbot/factory_girl

FactoryGirl.define do
  factory :audit do
    user "MyString"
    mutation 1
    model_affected 1
    action "MyString"
  end
end
