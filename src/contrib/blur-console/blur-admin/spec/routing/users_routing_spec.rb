require "spec_helper"

describe UsersController do
  describe "routing" do
    it "base routes to #index" do
      get("/users").should route_to(:controller => "users", :action => "index")
    end

    it "put to preferences#update should route to update" do
      put("/users/1/preferences/column").should route_to(:controller => "preferences", :action => "update", :user_id => '1', :pref_type => 'column')
    end

    it "post to the base uri routes to #create" do
      post("/users").should route_to(:controller => "users", :action => "create")
    end

    it "get new should route to #new" do
      get("/users/new").should route_to(:controller => "users", :action => "new")
    end

    it "get edit should route to #edit" do
      get("/users/1/edit").should route_to(:controller => "users", :action => "edit", :id => '1')
    end

    it "get show should route to #show" do
      get("/users/1").should route_to(:controller => "users", :action => "show", :id => '1')
    end

    it "put update should route to #update" do
      put("/users/1").should route_to(:controller => "users", :action => "update", :id => '1')
    end

    it "delete destroy should route to #destroy" do
      delete("/users/1").should route_to(:controller => "users", :action => "destroy", :id => '1')
    end
  end
end