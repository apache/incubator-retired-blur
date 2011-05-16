Rails.application.routes.draw do |map|
  namespace(:afs) do
    scope :afs do
      controller "file_systems" do
        match '/',    :to => :index,   :as => :index,   :via => :get
        match '/dir/expand/:fs/:level', :to => :dir_expand, :as => :dir_expand, :via => :get
        match '/view/:fs', :to => :view, :as => :view
      end
      
      match 'public/*file' => 'public#serve', :as => :public
    end
  end
end