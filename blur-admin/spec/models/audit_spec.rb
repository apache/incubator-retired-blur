require 'spec_helper'

describe Audit do
  describe "log_event" do
    it "should downcase the model and the mutation" do
      # This test is purely for string collision and readability
      user = FactoryGirl.create :user
      created_audit = Audit.log_event user, "Message", "MoDeL", "MuTaTiOn"
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
end
