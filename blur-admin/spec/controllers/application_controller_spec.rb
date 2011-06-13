require "spec_helper"

describe ApplicationController do
  describe "thrift_client" do
    it "makes the setup_thrift call if @client" do
      @client = mock(Blur::Blur::Client)
      controller.stub!(:setup_thrift)
      #get :thrift_client
    end
  end
end