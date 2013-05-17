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

  describe '404' do
    it 'should render the 404 page' do
      get :error_404
      response.should render_template('error_404')
    end
  end

  describe '422' do
    it 'should render the 422 page' do
      get :error_422
      response.should render_template('error_422')
    end
  end

  describe '500' do
    it 'should render the 500 page' do
      get :error_500
      response.should render_template('error_500')
    end
  end
end