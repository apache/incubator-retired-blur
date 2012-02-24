require 'spec_helper'

describe HdfsMetricsController do
	describe "requests" do
		before (:each) do
			@hdfs = Factory.create :hdfs
			@user = Factory.create :user
			post '/user_sessions', :user_session => {:username => @user.username, :password => 'password'}
		end

		describe "GET /hdfs_metrics" do
			it "should return a 200 response" do
				get hdfs_metrics_path
			  	response.status.should be(200)
			end
		end

		describe "PUT /disk" do
			it "should return a 200 response with and hdfs id" do
				put disk_usage_stats_path(@hdfs.id)
				response.status.should be(200)
			end

			it "should return a 200 response with and hdfs id and stat_days" do
				put disk_usage_stats_path(@hdfs.id), :stat_days => 1
				response.status.should be(200)
			end

			it "should return a 200 response with and hdfs id and stat_id" do
				put disk_usage_stats_path(@hdfs.id), :stat_id => 1
				response.status.should be(200)
			end
		end

		describe "PUT /nodes" do
			it "should return a 200 response with and hdfs id" do
				put node_stats_path(@hdfs.id)
				response.status.should be(200)
			end

			it "should return a 200 response with and hdfs id and stat_days" do
				put node_stats_path(@hdfs.id), :stat_days => 1
				response.status.should be(200)
			end

			it "should return a 200 response with and hdfs id and stat_id" do
				put node_stats_path(@hdfs.id), :stat_id => 1
				response.status.should be(200)
			end
		end

		describe "PUT /nodes" do
			it "should return a 200 response with and hdfs id" do
				put block_stats_path(@hdfs.id)
				response.status.should be(200)
			end

			it "should return a 200 response with and hdfs id and stat_days" do
				put block_stats_path(@hdfs.id), :stat_days => 1
				response.status.should be(200)
			end

			it "should return a 200 response with and hdfs id and stat_id" do
				put block_stats_path(@hdfs.id), :stat_id => 1
				response.status.should be(200)
			end
		end
	end
end
