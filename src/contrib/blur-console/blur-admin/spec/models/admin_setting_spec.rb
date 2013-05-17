require 'spec_helper'

describe AdminSetting do
  describe "search filter" do
    it "should create a new setting when one doesnt exist" do
      before_count = AdminSetting.all.count
      AdminSetting.search_filter
      before_count.should == AdminSetting.all.count - 1
    end

    it "should return a setting when one does exist" do
      setting = FactoryGirl.create :admin_setting, :setting => "regex_filter"
      AdminSetting.search_filter.should == setting
    end
  end
end