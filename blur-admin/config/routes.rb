BlurAdmin::Application.routes.draw do
  resources :hdfs_metrics, :only => [:index]
  match 'hdfs_metrics/:id/stats' => 'hdfs_metrics#stats', :via => :put, :as => :hdfs_stats

  resources :user_sessions, :only => [:create]
  match 'login' => 'user_sessions#new', :as => :login
  match 'logout' => 'user_sessions#destroy', :as => :logout

  resources :users do
    match '/preferences/:pref_type' => 'preferences#update', :via => :put, :as => :preference
  end

  resource :search, :controller => 'search'
  match 'search/load/:search_id' => 'search#load', :as => :search_load
  match 'search/delete/:search_id/:blur_table' => 'search#delete', :via => :delete, :as => :delete_search
  match 'search/:search_id/:blur_table' => 'search#create', :via => :get, :as => :fetch_results
  match 'search/save/' => 'search#save', :via => :post
  match 'search/:search_id' => 'search#update', :via => :put, :as => :update_search
  match 'search/:blur_table_id/filters' => 'search#filters' , :via => :put, :as => :search_filters
  match 'reload/:blur_table' => 'search#reload'

  resources :zookeepers, :only => :index
  match 'zookeeper' => 'zookeepers#show_current', :as => :zookeeper
  match 'zookeepers/make_current' => 'zookeepers#make_current', :via => :put, :as => :make_current_zookeeper
  match 'zookeepers/dashboard' => 'zookeepers#dashboard', :via => :get, :as => :dashboard
  match 'zookeepers/:id' => 'zookeepers#show', :via => :get, :as => :show_zookeeper
  match 'zookeepers/:id/controller/:controller_id' => 'zookeepers#destroy_controller', :via => :delete, :as => :destroy_controller
  match 'zookeepers/:id/shard/:shard_id' => 'zookeepers#destroy_shard', :via => :delete, :as => :destroy_shard
  match 'zookeepers/:id/cluster/:cluster_id' => 'zookeepers#destroy_cluster', :via => :delete, :as => :destroy_cluster
  match 'zookeepers/:id/' => 'zookeepers#destroy_zookeeper', :via => :delete, :as => :destroy_zookeeper

  resources :blur_tables, :except => [:new, :create, :edit, :show, :update, :destroy] do
    put 'enable', :on => :collection, :as => :enable_selected
    put 'disable', :on => :collection, :as => :disable_selected
    delete 'forget', :on => :collection, :as => :forget_selected
    delete 'destroy', :on => :collection, :as => :destroy_selected
    get 'hosts', :on => :member
    get 'schema', :on => :member
    get 'reload', :on => :collection, :as => :reload
    post 'terms', :on => :member
  end

  match 'blur_queries/refresh/:time_length' => 'blur_queries#refresh', :via => :get, :as => :refresh
  resources :blur_queries, :only => [:index, :update] do
    member do
      get 'more_info'
      get 'times'
    end
  end

  match 'hdfs(/:id(/show(*fs_path)))' => 'hdfs#index', :via => :get, :as => :hdfs
  match 'hdfs/:id/info' => 'hdfs#info', :via => :get, :as => :hdfs_info
  match 'hdfs/:id/folder_info' => 'hdfs#folder_info', :via=>:get, :as => :hdfs_folder_info
  match 'hdfs/:id/slow_folder_info' => 'hdfs#slow_folder_info', :via=>:get, :as => :hdfs_slow_folder_info
  match 'hdfs/:id/expand(*fs_path)' => 'hdfs#expand', :via => :get, :as => :hdfs_expand, :format => false
  match 'hdfs/:id/file_info(*fs_path)' => 'hdfs#file_info', :via => :get, :as => :hdfs_file_info, :format => false
  match 'hdfs/:id/move' => 'hdfs#move_file', :via => :post, :as => :hdfs_move
  match 'hdfs/:id/mkdir' => 'hdfs#mkdir', :via => :post, :as => :hdfs_mkdir
  match 'hdfs/:id/delete_file' => 'hdfs#delete_file', :via => :post, :as => :hdfs_delete
  match 'hdfs/:id/upload_form' => 'hdfs#upload_form', :via => :get, :as => :hdfs_upload_form
  match 'hdfs/:id/upload/' => 'hdfs#upload', :via =>:post, :as => :hdfs_upload
  match 'hdfs/:id/structure' => 'hdfs#file_tree', :via =>:get, :as => :hdfs_structure

  match 'help/:tab' => 'application#help', :as => :help
  root :to => 'zookeepers#index'
end
