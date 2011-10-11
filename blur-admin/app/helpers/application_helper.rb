module ApplicationHelper
  def options_for_created_at_time(default=0)
    options_for_select([['1 minute', 1],
                        ['5 minutes', 5],
                        ['15 minutes', 15],
                        ['30 minutes', 30],
                        ['1 hour', 60]],
                       default)
  end

  def options_for_all_yes_no(default)
    options_for_select([['All Queries', nil],
                        ['Yes', true],
                        ['No', false]],
                       default)
  end

  def options_for_all_on_off(default)
    options_for_select([['All Queries', nil],
                        ['On', true],
                        ['Off', false]],
                       default)
  end
  def options_for_refresh_period(default='false')
    options_for_select([['never', 'false'],
                        ['continuously', 'continuous'],
                        ['every 10 seconds', 10],
                        ['every 1 minute', 60],
                        ['every 10 minutes', 600]],
                       default)
  end
end
