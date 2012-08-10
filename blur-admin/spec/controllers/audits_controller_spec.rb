require 'spec_helper'

describe AuditsController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests
    end

    describe 'GET index' do
      it "renders the index template" do
        get :index
        response.should render_template 'index'
      end

      it "grabs all audits within the last two days without given hours" do
        Audit.should_receive(:recent).with(48)
        get :index
      end

      it "grabs all audits within the last hours given the hours" do
        Audit.should_receive(:recent).with(40)
        get :index, :hours => 40
      end
    end
  end
end
