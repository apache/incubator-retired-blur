require 'spec_helper'
describe "Navigation Bar" do
  before(:each) do
    setup_tests
  end
  context "Click on Blur Logo" do
    it "goes to the dashboard pages" do
      visit zookeepers_path
      find('a', :class => 'brand').click
      current_path.should == root_path
    end
  end
  context "Click on Dashboard" do
    it "goes to dashboard page" do
      visit zookeepers_path
      click_on 'Dashboard'
      current_path.should == root_path
    end
  end
  context "Click on Environment Status" do
    it "goes to the environment status page" do
      click_on 'env_link'
      current_path.should == zookeeper_path(@zookeeper.id)
    end
  end
  context "Click on Blur Tables" do
    it "goes to the blur tables page" do
      click_on 'tables_link'
      current_path.should == zookeeper_blur_tables_path(@zookeeper.id)
      #current_path.should == zookeeper_path(@zookeeper.id) + '/blur_tables'
    end
  end
  context "Click on Blur Queries" do
    it "goes to the blur queries page" do
      click_on 'queries_link'
      current_path.should == zookeeper_blur_queries_path(@zookeeper.id)
    end
  end
  context "Click on Search" do
    it "goes to the search page" do
      click_on 'search_link'
      current_path.should == zookeeper_searches_path(@zookeeper.id)
    end
  end
  context "Click on HDFS File Browser" do
    it "goes to the HDFS file browser page" do
      click_on 'HDFS File Browser'
      current_path.should == hdfs_index_path
    end
  end
  context "Click on HDFS Metrics" do
    it "goes to te HDFS file metric page" do
      click_on 'HDFS Metrics'
      current_path.should == hdfs_metrics_path
    end
  end
  context "Click on Audits" do
    it "goes to the audits page" do
      click_on 'Audits'
      current_path.should == audits_path
    end
  end
  context "Click on Admin" do
    it "goes to the admin page" do
      click_on 'Admin'
      current_path.should == users_path
    end
  end
  context "Click on Account" do
    it "goes to the account page" do
      click_on 'Account'
      current_path.should == user_path(@user.id)
    end
  end
  context "Click on Log Out" do
    it "logs the user out and redirects to the login page" do
      click_on 'Log Out'
      current_path.should == login_path
    end
  end
end
