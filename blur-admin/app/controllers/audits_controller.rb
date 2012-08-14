class AuditsController < ApplicationController
  def index
    # If a range is given then us it to get the recent audits
    # Otherwise use the default (2 days)
    from = params[:from] || 48
    to = params[:to] || 0
    @audits = Audit.recent from.to_i, to.to_i

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
