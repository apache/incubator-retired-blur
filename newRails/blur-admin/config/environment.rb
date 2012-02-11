# Load the rails application
require File.expand_path('../application', __FILE__)

# Initialize the rails application
BlurAdmin::Application.initialize!

#generate the routes file
JsRoutes.generate!

#setup authlogic
BlurAdmin::Application.configure do
  config.gem "authlogic"
end