BlurAdmin::Application.routes.draw do
  resources :user_sessions, :only => [:create]

  resources :users do
    match '/preferences/:pref_type' => 'preferences#update', :via => :put, :as => :preference
  end

  resource :search, :controller => 'search'

  resources :zookeepers, :only => :index
  match 'zookeeper' => 'zookeepers#show_current', :as => :zookeeper
  match 'zookeepers/make_current' => 'zookeepers#make_current', :via => :put, :as => :make_current_zookeeper
  match 'zookeepers/dashboard' => 'zookeepers#dashboard', :via => :get, :as => :dashboard
  match 'zookeepers/:id' => 'zookeepers#show', :via => :get, :as => :show_zookeeper
  match 'zookeepers/:id/controller/:controller_id' => 'zookeepers#destroy_controller', :via => :delete, :as => :destroy_controller
  match 'zookeepers/:id/shard/:shard_id' => 'zookeepers#destroy_shard', :via => :delete, :as => :destroy_shard

  resources :blur_tables do
    get 'hosts', :on => :member
    get 'schema', :on => :member
    get 'reload', :on => :collection, :as => :reload
    put 'update_all', :on => :collection
    delete 'delete_all', :on => :collection
  end

  match 'blur_queries/refresh' => 'blur_queries#refresh', :via => :get, :as => :refresh
  resources :blur_queries do
    member do
      get 'more_info'
      get 'times'
    end
  end

  controller "search" do
    match 'search/:blur_table_id/filters', :to => :filters, :as => :search_filters, :via => :get
  end

  match 'login' => 'user_sessions#new', :as => :login
  match 'logout' => 'user_sessions#destroy', :as => :logout
  match 'search/load/:search_id' => 'search#load', :as => :search_load
  match 'search/delete/:search_id/:blur_table' => 'search#delete', :via => :delete, :as => :delete_search
  match 'search/:search_id/:blur_table' => 'search#create', :as => :fetch_results
  match 'search/save/' => 'search#save', :via => :post
  match 'search/:search_id' => 'search#update', :via => :put, :as => :update_search
  match 'reload/:blur_table' => 'search#reload'
  match 'help/:tab' => 'application#help', :as => :help

  match 'hdfs' => 'hdfs#index', :via => :get
  match 'hdfs/:id/info' => 'hdfs#info', :via => :get, :as => :hdfs_info
  match 'hdfs/:id/expand(*fs_path)' => 'hdfs#expand', :via => :get, :as => :hdfs_expand
  match 'hdfs/:id/file_info(*fs_path)' => 'hdfs#file_info', :via => :get, :as => :hdfs_file_info
  match 'hdfs/:id/move' => 'hdfs#move_file', :via => :post, :as => :hdfs_move
  match 'hdfs/:id/delete_file' => 'hdfs#delete_file', :via => :post, :as => :hdfs_delete

  root :to => 'zookeepers#index'

  # The priority is based upon order of creation:
  # first created -> highest priority.

  # Sample of regular route:
  #   match 'products/:id' => 'catalog#view'
  # Keep in mind you can assign values other than :controller and :action

  # Sample of named route:
  #   match 'products/:id/purchase' => 'catalog#purchase', :as => :purchase
  # This route can be invoked with purchase_url(:id => product.id)

  # Sample resource route (maps HTTP verbs to controller actions automatically):
  #   resources :products

  # Sample resource route with options:
  #   resources :products do
  #     member do
  #       get 'short'
  #       post 'toggle'
  #     end
  #
  #     collection do
  #       get 'sold'
  #     end
  #   end

  # Sample resource route with sub-resources:
  #   resources :products do
  #     resources :comments, :sales
  #     resource :seller
  #   end

  # Sample resource route with more complex sub-resources
  #   resources :products do
  #     resources :comments
  #     resources :sales do
  #       get 'recent', :on => :collection
  #     end
  #   end

  # Sample resource route within a namespace:
  #   namespace :admin do
  #     # Directs /admin/products/* to Admin::ProductsController
  #     # (app/controllers/admin/products_controller.rb)
  #     resources :products
  #   end

  # You can have the root of your site routed with "root"
  # just remember to delete public/index.html.

  # See how all your routes lay out with "rake routes"

  # This is a legacy wild controller route that's not recommended for RESTful applications.
  # Note: This route will make all actions in every controller accessible via GET requests.
  # match ':controller(/:action(/:id(.:format)))'
end
