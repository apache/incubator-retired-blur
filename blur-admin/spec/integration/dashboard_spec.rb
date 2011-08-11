require 'spec_helper'

describe "dashboard" do
  # generate a valid user
  let(:user) { Factory.create :user }

  before do
    @zookeepers = Array.new(5).collect {Factory.create :zookeeper_with_blur_tables}
    visit login_path
    fill_in 'Username', :with => user.username
    fill_in 'Password', :with => user.password
    click_button 'Log In'
    visit root_path
  end

  it "displays the name of every zookeeper" do
    @zookeepers.each do |zookeeper|
      page.should have_content zookeeper.name
    end
  end

  it "displays the status of every zookeeper" do
    @zookeepers.each do |zookeeper|
      status = case zookeeper.status
                 when 0 then "Offline"
                 when 1 then "Online"
                 else        "Not Available"
               end

      find("table##{zookeeper.id} div.zookeeper-status").should have_content status
    end
  end

end
