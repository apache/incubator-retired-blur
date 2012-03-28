require "spec_helper"

describe UserSessionsController do
  describe "routing" do
    it "base routes to #create" do
      post("/user_sessions").should route_to(:controller => "user_sessions", :action => "create")
    end

    it "login path routes to login" do
      get("/login").should route_to(:controller => "user_sessions", :action => "new")
    end

    it "nodes routes to #live_dead_nodes" do
      get("/logout").should route_to(:controller => "user_sessions", :action => "destroy")
    end
  end
end
