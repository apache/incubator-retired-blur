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
      pending "need to implement"
    end
  end
end
