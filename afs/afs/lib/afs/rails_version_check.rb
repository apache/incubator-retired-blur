unless Rails.version >= '3.0'
  msg = []
  msg << "It seems you are not using Rails 3."
  msg << "Rails 3 is required to use this plugin."
  raise msg.join(' ')
end