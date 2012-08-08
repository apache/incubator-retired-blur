require 'spec_helper'

describe Audit do
  describe "log_event" do
    it "should downcase the model and the mutation" do
      user = FactoryGirl.create :user
      # This test is purely for string collision and readability
      created_audit = Audit.log_event user, "Message", "MoDeL", "MuTaTiOn"
      created_audit.mutation.should == "mutation"
      created_audit.model_affected == "model"
    end
  end
end
