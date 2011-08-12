require 'spec_helper'

describe "environment status" do
  # generate a valid user
  let(:user) { Factory.create :user, :editor => '1' }
  # generate a current zookeeper
  let(:zookeeper) {Factory.create :zookeeper_with_blur_tables}

  before do
    visit login_path
    fill_in 'Username', :with => user.username
    fill_in 'Password', :with => user.password
    click_button 'Log In'
    visit "/zookeepers/#{zookeeper.id}" # set current zookeeper
    visit blur_tables_path
  end

  it "displays the table name, number of hosts, shards, records, queries/second, and size in the header" do
    zookeeper.blur_tables.each do |table|
      find("#blur_table_#{table.id} > h3").should have_content table.table_name
      find("#blur_table_#{table.id} > h3").should have_content table.hosts.keys.length
      find("#blur_table_#{table.id} > h3").should have_content table.hosts.values.flatten.length
      find("#blur_table_#{table.id} > h3").should have_content table.query_usage
      find("#blur_table_#{table.id} > h3").should have_content table.is_enabled? ? "Enabled" : "Disabled"
    end
  end

  it "displays the location and any relevent action buttons in the body" do
    zookeeper.blur_tables.each do |table|
      find("#blur_table_#{table.id}").should have_content table.table_uri
      if table.is_enabled?
        find("#blur_table_#{table.id}").should have_selector "input[value='Disable Table']"
      else
        find("#blur_table_#{table.id}").should have_selector "input[value='Enable Table']"
        find("#blur_table_#{table.id}").should have_selector "input[value='Delete Table']"
      end
    end
  end
end
