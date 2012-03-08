require 'spec_helper'
include ApplicationHelper

describe LicenseHelper do
  before(:each) do
    @license = FactoryGirl.create :license
  end
  describe "license text" do
    before(:each) do
      @prefix = 'Licensed to: NIC on 08 Sep 2011.'
    end

    describe 'License expiration' do
      it 'should state no warning when the license will expire in more than 30 days' do
        license_text(@license).should == @prefix
      end

      it 'should return No license found when the license is nil' do
        license_text(nil).should == 'No valid license found.'
      end

      it 'should state that the license is expired when the number of days left is less than 0' do
        @license.stub!(:expires_date).and_return Date.today.months_ago(1)
        license_text(@license).should == @prefix + ' License is expired.'
      end

      it 'should state that the license is about to expire today when the number of days left is 0' do
        @license.stub!(:expires_date).and_return Date.today
        license_text(@license).should == @prefix + ' License expires today.'
      end

      it 'should state that the license will expire in x days when the number of days left is x and x is less than 30' do
        @license.stub!(:expires_date).and_return Date.today.weeks_ago(-1)
        license_text(@license).should == @prefix + ' License expires in 7 days.'
      end
    end

    describe 'Node Overage' do
      before(:each) do
        @license.stub!(:node_overage).and_return 1
      end

      it 'should warn that you have a node overage and if you are over teh grace period then tell you to contact nic' do
        @license.stub!(:grace_period_days_remain).and_return -2
        license_text(@license).should == @prefix + ' There is currently 1 node over the licensed amount. Please contact Near Infinity to upgrade the license.'
      end

      it 'should warn that you have a node overage and get the correct pluralization' do
        @license.stub!(:node_overage).and_return 2
        @license.stub!(:grace_period_days_remain).and_return -2
        license_text(@license).should == @prefix + ' There are currently 2 nodes over the licensed amount. Please contact Near Infinity to upgrade the license.'
      end

      it 'should warn that you have a node overage and if you are within the grace period then it should tell you how many days are left' do
        @license.stub!(:grace_period_days_remain).and_return 3
        license_text(@license).should == @prefix + ' There is currently 1 node over the licensed amount. 3 days left until a new license is needed.'
      end
    end

    describe 'cluster overage' do
      it 'should warn of a cluster overage if there are any clusters that have expired' do
        @license.stub!(:cluster_overage).and_return 1
        license_text(@license).should == @prefix + ' There is currently 1 cluster over the licensed amount. Please contact Near Infinity to upgrade the license.'
      end

      it 'should warn of a cluster overage if there are any clusters that have expired with correct pluralization' do
        @license.stub!(:cluster_overage).and_return 2
        license_text(@license).should == @prefix + ' There are currently 2 clusters over the licensed amount. Please contact Near Infinity to upgrade the license.'
      end
    end
  end

  describe "footer class" do
    it 'should return false if the license is fine' do
      footer_class(@license).should == false
    end

    it 'should return expiring_license if the license is nil' do
      footer_class(nil).should == 'expiring_license'
    end

    it 'should return expiring_license if it is within 30 days of expiring or has expired, or if there is a cluster overage' do 
      @license.stub!(:expires_date).and_return Date.today.weeks_ago(-1)
      footer_class(@license).should == 'expiring_license'
    end

    it 'should return expiring_license if there is a cluster overage' do 
      @license.stub!(:cluster_overage).and_return 1
      footer_class(@license).should == 'expiring_license'
    end
  end
end