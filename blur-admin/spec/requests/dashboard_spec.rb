require 'spec_helper'

describe "Dashboard" do
  before(:each) do 
    setup_tests
  end
  context "Click on a Blur Instance picture" do
    it "goes to the environment status page for that zookeeper" do
      save_and_open_page
      find('a', :class => 'zookeeper-body').click
      puts current_path
    end
  end
end