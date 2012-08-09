class AuditsController < ApplicationController
  def index
    # If a range is given then us it to get the recent audits
    # Otherwise use the default (2 days)
    @audits = Audit.recent params[:hours].to_i || 48

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
