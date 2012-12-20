class AdminSetting < ActiveRecord::Base
  validates :setting, :uniqueness => true
end
