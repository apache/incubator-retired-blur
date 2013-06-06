BlurAdmin::Application.configure do
  # Settings specified here will take precedence over those in config/application.rb

  # Code is not reloaded between requests
  config.cache_classes = true

  # Full error reports are disabled and caching is turned on
  config.consider_all_requests_local       = false
  config.action_controller.perform_caching = true

  # Disable Rails's static asset server (Apache or nginx will already do this)
  config.serve_static_assets = false

  # Compress JavaScripts and CSS
  config.assets.compress = true

  # Don't fallback to assets pipeline if a precompiled asset is missed
  config.assets.compile = false

  # Generate digests for assets URLs
  config.assets.digest = true
  
  config.assets.precompile += Dir.foreach('app/assets/javascripts/').select{|file| (file =~ /.js/) }
  config.assets.precompile += ['blur_table/blur_tables.js', 'dashboard/dashboard.js', 'environment/environment.js']
  
  #.reject{|file| (file =~ /.*\.coffee/).nil? && (file =~ /routes.js/).nil?}.collect{|file| file.gsub /.coffee/, ''}
  
  config.action_dispatch.x_sendfile_header = "X-Sendfile"
  
  class LogFormatter < Logger::Formatter
    def call(severity,time,progname,msg)
      "#{severity} #tok1-block-tok #{msg}\n"
    end
  end
  config.logger = Logger.new("#{::Rails.root.to_s}/log/#{ENV['RAILS_ENV']}.log", 10,26_214_400)
  config.logger.formatter = LogFormatter.new
  config.logger.level = Logger::WARN

  config.i18n.fallbacks = true

  # Send deprecation notices to registered listeners
  config.active_support.deprecation = :notify

  # Log the query plan for queries taking more than this (works
  # with SQLite, MySQL, and PostgreSQL)
  config.active_record.auto_explain_threshold_in_seconds = 2

  config.action_dispatch.rack_cache = {:metastore => "rails:/", :entitystore => "rails:/", :verbose => false}
end
