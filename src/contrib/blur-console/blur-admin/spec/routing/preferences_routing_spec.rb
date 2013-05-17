require "spec_helper"

describe PreferencesController do
  describe "routing" do
    it "update routes to #update" do
      put("/users/1/preferences/column").should route_to(:controller => "preferences", :action => "update", :user_id =>'1', :pref_type => 'column')
    end
  end
end