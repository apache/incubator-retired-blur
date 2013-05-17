require 'spec_helper'

describe Audit do
  describe "log_event" do
    it "should downcase the model and the mutation" do
      # This test is purely for string collision and readability
      user = FactoryGirl.create :user
      zookeeper = FactoryGirl.create :zookeeper
      created_audit = Audit.log_event user, "Message", "MoDeL", "MuTaTiOn", zookeeper
      created_audit.mutation.should == "mutation"
      created_audit.model_affected == "model"
    end
  end

  describe "scope" do
    it "should return the audits within the given time range" do
      # Create a set of queries with different created at times
      FactoryGirl.create :audit, :created_at => 10.days.ago
      returned = [FactoryGirl.create(:audit, :created_at => 1.days.ago)]

      # Grab all the audits within the last 72 hours
      recent = Audit.recent 72, 0
      recent.should == returned
    end
  end

  describe "summary" do
    it 'should return a hash with the correct data' do
      audit = FactoryGirl.create :audit
      summary = audit.summary
      summary.should include(:action)
      summary.should include(:date_audited)
      summary.should include(:model)
      summary.should include(:mutation)
      summary.should include(:username)
      summary.should include(:user)
      summary.should include(:zookeeper_affected)
    end
  end
end
