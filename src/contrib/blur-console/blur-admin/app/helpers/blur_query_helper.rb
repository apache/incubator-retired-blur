module BlurQueryHelper
  def format_title(query_string)
    query_string.length() > 20 ? query_string.gsub(/ \+/) {'<br />+'} : ''
  end

  def print_value(conditional, default_message = "Not Available")
    return default_message unless conditional
    return conditional unless block_given?
    yield
  end
end
