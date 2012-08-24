BlurAdmin::Application.routes.draw do
  resources :users do
    match '/preferences/:pref_type' => 'preferences#update', :via => :put, :as => :preference
  end

  resources :zookeepers, :only => [:index, :show, :destroy], :shallow => true do
    #Zookeeper routes
    #Nested cluster Resource
    resources :clusters, :only => [:destroy] do
      resources :shards, :only => [:index, :destroy]
    end

    #Nested Controller Resource
    resources :shards, :only => [:destroy]


    member do
      get 'long_running' => 'zookeepers#long_running_queries', :as => :long_running_queries
    end

    #Nested Search Resource
    resources :searches, :only => [:index, :update] do
      member do
        post 'load'
        delete 'delete/:blur_table', :action => :delete, :as => :delete
      end

      collection do
        post 'save'
        post ':blur_table', :action => :create, :as => :fetch_results
        get 'filters/:blur_table', :action => :filters, :as => :filters
      end
    end

    #Nested BlurTables Resource
    resources :blur_tables, :only => :index do
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

    #Nested BlurQueries Resource
    resources :blur_queries, :only => [:index, :update] do
      member do
        get 'more_info'
      end

      collection do
        get 'refresh/:time_length', :action => :refresh, :as => :refresh
      end
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

  match 'login' => 'user_sessions#new', :as => :login
  match 'logout' => 'user_sessions#destroy', :as => :logout
  match 'help/:tab' => 'application#help', :as => :help
  root :to => 'zookeepers#index'
end
