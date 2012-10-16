require 'spec_helper'

describe "Blur Queries" do
  before(:each) do
    setup_tests
    #    wait_for_ajax
    #wait_for_dom
  end
  context "Page is loaded" do
    it "should display the correct page elements" do
      visit zookeeper_blur_queries_path(@zookeeper.id)
      page.should have_table('queries-table')
      page.should have_content('User ID')
      page.should have_content('Query')
      page.should have_content('Table Name')
      page.should have_content('Starting Record')
      page.should have_content('Time Submitted')
      page.should have_content('Status')
      page.should have_content('Actions/Info')
      page.should have_css('div', :class => 'range_select')
      save_and_open_page
    end
  end
end