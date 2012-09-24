BlurAdmin::Application.routes.draw do
  # User routes
  resources :users do
    match '/preferences/:pref_type' => 'preferences#update', :via => :put, :as => :preference
  end

  # Zookeeper routes
  resources :zookeepers, :only => [:index, :show, :destroy], :shallow => true do
    # Nested cluster Resource
    resources :clusters, :only => [:destroy] do
      # Nested Shards Resource
      resources :blur_shards, :only => [:index, :destroy]
    end

    # Nested BlurTables Resource
    resources :blur_tables, :only => :index do

      # BlurTable routes
      member do
        get 'terms'
        put 'comment'
      end
      collection do
        put 'enable'
        put 'disable'
        delete 'destroy'
      end
    end

    # Nested BlurQueries Resource
    resources :blur_queries, :only => [:index, :show] do

      #Blur Queries routes
      member do
        put 'cancel'
      end
      collection do
        get 'refresh/:time_length', :action => :refresh, :as => :refresh
      end
    end

    # Nested Controller Resource
    resources :blur_controllers, :only => [:destroy]

    # Nested Search Resource
    resources :searches, :only => [:index, :update, :show, :destroy] do
      collection do
        post 'save'
        post ':blur_table', :action => :create, :as => :fetch_results
        get 'filters/:blur_table', :action => :filters, :as => :filters
      end
    end

    # Zookeeper specific action
    member do
      get 'long_running' => 'zookeepers#long_running_queries', :as => :long_running_queries
    end
  end

  resources :hdfs, :only => :index do
    get '(/:id(/show(*fs_path)))', :action => :index, :on => :collection
    member do
      get 'info'
      get 'folder_info'
      get 'slow_folder_info'
      get 'expand(*fs_path)', :action => :expand, :as => :expand, :format => false
      get 'file_info(*fs_path)', :action => :file_info, :as => :file_info, :format => false
      post 'move', :action => :move_file
      post 'mkdir'
      post 'delete_file'
      get 'upload_form'
      post 'upload'
      get 'structure', :action => :file_tree

    end
  end

  resources :user_sessions, :only => [:create]
  resources :hdfs_metrics, :only => [:index] do
    member do
      get 'stats', :action => :stats
    end
  end

  resources :audits, :only => [:index]

  #Errors
  match '/404' => 'errors#error_404'
  match '/500' => 'errors#error_500'
  match '/422' => 'errors#error_422'

  match 'login' => 'user_sessions#new', :as => :login
  match 'logout' => 'user_sessions#destroy', :as => :logout
  match 'help/:tab' => 'application#help', :as => :help
  root :to => 'zookeepers#index'
end
