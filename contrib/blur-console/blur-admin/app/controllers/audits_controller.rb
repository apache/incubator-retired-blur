class AuditsController < ApplicationController
  respond_to :json, :html

  def index
    from = params[:from] || 48  # Use the given min time or the default 48 hours
    to = params[:to] || 0       # Use the given max time or the default (now)
    @audits = Audit.recent from.to_i, to.to_i

    respond_with(@audits)
  end
end
