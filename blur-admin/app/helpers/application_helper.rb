module ApplicationHelper
  def include_safe_javascript
    begin
      render 'javascript'
    rescue ActionView::MissingTemplate
      nil
    end
  end
end
