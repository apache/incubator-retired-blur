require 'spec_helper'

describe AdminSettingsController do
  describe "actions" do
    before do
      # Universal setup
      setup_tests
    end

    describe 'PUT update' do
      it 'should create a new admin setting when a previous one DNE' do
        initial_count = AdminSetting.all.length
        put :update, :setting => 'regex'
        AdminSetting.all.length.should == initial_count + 1
      end

      it 'should find an admin setting when a previous one exists' do
        setting = FactoryGirl.create :admin_setting
        initial_count = AdminSetting.all.length
        AdminSetting.should_receive(:find_or_create_by_setting).with('regex').and_return(setting)
        put :update, :setting => 'regex'
        AdminSetting.all.length.should == initial_count
      end

      it 'should update the setting to the given value' do
        setting = FactoryGirl.create :admin_setting
        AdminSetting.should_receive(:find_or_create_by_setting).with('regex').and_return(setting)
        put :update, :setting => 'regex', :value => 'test'
        setting.value.should == 'test'
      end

      it 'should succeed with a given setting' do
        expect {
          put :update, :setting => 'regex'
        }.to_not raise_error
      end

      it 'should fail when a setting is not given' do
        expect {
          put :update
        }.to raise_error
      end
    end
  end
end