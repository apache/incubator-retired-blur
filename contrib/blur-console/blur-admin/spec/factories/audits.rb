# Read about factories at https://github.com/thoughtbot/factory_girl

FactoryGirl.define do
  factory :audit do
    user            { FactoryGirl.create :user }
    mutation        1
    model_affected  1
    action          "This is an action"
  end
end
