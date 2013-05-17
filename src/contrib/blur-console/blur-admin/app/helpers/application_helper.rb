module ApplicationHelper
  def pluralize_no_count(count, singular, plural = nil)
    ((count == 1 || count == '1') ? singular : (plural || singular.pluralize))
  end

  def stateful_nav_url(page)
    return '' if session[:current_zookeeper_id].nil?
    case page
    when 'environment'
      return zookeeper_path(session[:current_zookeeper_id])
    when 'blur_table'
      return zookeeper_blur_tables_path(session[:current_zookeeper_id])
    when 'blur_query'
      return zookeeper_blur_queries_path(session[:current_zookeeper_id])
    when 'search'
      return zookeeper_searches_path(session[:current_zookeeper_id])
    else
      return ''
    end
  end
end