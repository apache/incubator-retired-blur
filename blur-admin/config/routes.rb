BlurAdmin::Application.routes.draw do
  resource :data
  resource :runtime, :controller => 'runtime'
  resource :config, :controller => 'config'
	resource :query, :controller => 'query'
	resource :env, :controller => 'env'
  
  controller "runtime" do
     match '/queries/current/:table', :to => :current_queries, :as => :current_queries, :via => :get
  end

  root :to => "env#show"
end
