class AdminSetting < ActiveRecord::Base
  validates :setting, :uniqueness => true

  def self.search_filter
    AdminSetting.where(:setting => 'regex_filter').first_or_create(:value => '*')
  end
end
