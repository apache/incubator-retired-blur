Rails.application.routes.draw do |map|
  namespace(:afs) do
    scope :afs do
      controller "file_systems" do
        match '/',    :to => :index,   :as => :index,   :via => :get
      end
      
      match 'public/*file' => 'public#serve', :as => :public
    end
  end
end