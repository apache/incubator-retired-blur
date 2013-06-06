require 'spec_helper'

describe "Audits" do
  before(:each) do
    setup_tests
    visit audits_path
  end
  context "Page is loaded" do
    it "should show the audits table" do
      page.should have_css('div', :id => 'audits_table')
    end
    it "should have the correct table headers" do
      page.should have_content('Action Taken')
      page.should have_content('Username')
      page.should have_content('User')
      page.should have_content('Zookeeper/Root Path')
      page.should have_content('Model Affected')
      page.should have_content('Mutation Type')
      page.should have_content('Date')
    end
  end
end
