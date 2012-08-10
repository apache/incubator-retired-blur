class AuditsController < ApplicationController
  def index
    # If a range is given then us it to get the recent audits
    # Otherwise use the default (2 days)
    hours = params[:hours] || 48
    @audits = Audit.recent hours.to_i

    puts @audits.length

    respond_to do |format|
      format.html
      format.json { render :json => @audits.as_json(
        :include =>
          {:user =>
            {:only => :username}
          }
        )
      }
    end
  end
end
