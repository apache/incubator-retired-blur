require 'spec_helper'

describe "blur queries" do
  # generate a valid user
  let(:user) { Factory.create :user_with_preferences }
  # generate a current zookeeper
  let(:zookeeper) {Factory.create :zookeeper_with_blur_queries}

  before do
    visit login_path
    fill_in 'Username', :with => user.username
    fill_in 'Password', :with => user.password
    click_button 'Log In'
    visit "/zookeepers/#{zookeeper.id}" # set current zookeeper
    visit blur_queries_path
  end

  it "shows filter options" do
    pending "New Table Implementation"
    find("#filter_form").should have_content "Within past:"
    find("#filter_form").should have_content "Super Query:"
    find("#filter_form").should have_content "Query State:"
  end

  it "shows table and refresh options" do
    pending "New Table Implementation"
    find("#table_wrapper").should have_content "Blur Table:"
    find("#refresh_wrapper").should have_content "Refresh:"
  end
end
