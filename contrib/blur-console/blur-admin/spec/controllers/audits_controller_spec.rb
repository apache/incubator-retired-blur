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
        Audit.should_receive(:recent).with(48, 0)
        get :index
      end

      it "grabs all audits within the last hours given the from hours" do
        Audit.should_receive(:recent).with(40, 0)
        get :index, :from => 40
      end

      it "grabs all audits within the last hours given the to hours" do
        Audit.should_receive(:recent).with(48, 10)
        get :index, :to => 10
      end

      it "grabs all audits within the the given range of hours" do
        Audit.should_receive(:recent).with(50, 20)
        get :index, :from => 50, :to => 20
      end
    end
  end
end
