require 'spec_helper'

describe "Search" do
  before(:each) do
    setup_tests
    click_on 'search_link'
  end
  context "Page is loaded" do
    it "should show the query string" do
      page.should have_field('query_string')
    end
    it "should have the submit button" do
      page.should have_button('search_submit')
    end
    it "should have the blur table drop down" do
      page.should have_select('blur_table')
    end
    it "should have the advanced search options" do
      page.should have_content('Column Families')
      page.should have_css('div', :class =>'dynatree-container')
      page.should have_content('Search On:')
      page.should have_field('search_row', :checked => true)
      page.should have_field('search_record', :checked => false)
      page.should have_field('return_row', :checked => true)
      page.should have_field('return_record', :checked => false)
      page.should have_field('offset')
      page.should have_field('result_count')
    end
    it "should have the saved search drop down" do
      page.should have_css('div', :class => 'saved')
      page.should have_field('save_name')
      page.should have_button('save_button')
      page.should have_button('update_button')
    end
  end
end
