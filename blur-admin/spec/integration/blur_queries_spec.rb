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

  it "displays the queries that were just created" do
    zookeeper.blur_queries.each do |query|
      find("#queries-table").should have_content query.query_string.slice(0, 15)
      find("#queries-table").should have_content query.userid
    end
  end
end
