require 'spec_helper'

describe ErrorsController do
  describe 'actions' do
    before(:each) do
      setup_tests
    end

    it "renders a 404 page when the webpage is not found" do
      page = stub(:code => 404)
      expect {
        visit '/page_that_does_not_exist' 
      }.to raise_error ActionController::RoutingError

    end
  end
end