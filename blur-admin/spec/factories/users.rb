FactoryGirl.define do
	factory :user do
		sequence(:username)  	{|n| "user#{n}"}
		sequence(:name)      	{|n| "user#{n}"}
		sequence (:email)     {|n| "user#{n}@example.com"}
		password              "password"
		password_confirmation "password"
		roles                 {User.valid_roles}

    factory :user_with_preferences do
      after_create do |user|
        FactoryGirl.create_list(:preference, 1, :user => user)
      end
    end
  end
end