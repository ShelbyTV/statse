#!/usr/bin/env ruby
# encoding: UTF-8

# Run Frequency: Daily @ 12:01 AM
# This script harvests data from mongodb and sends it to a statsd server

require_relative 'config'

db = Mongo::Connection.new(MONGO_HOST, MONGO_PORT).db(DB_NAME)

####################################################
# define map and reduce functions
####################################################
map = <<-JS
      function(){
        emit(this._id, {count: 1});
      }
      JS

reduce = <<-JS
      function (key, values) {
        var sum = 0;
        values.forEach(function (f) {
          sum += f.count;
        });
        return {days_with_activity: sum};
      } 
      JS

####################################################
# get collection names of past 7 days
#  and run M/R on each collection
#  output goes to one collection, weekly:date
####################################################
today = Date.today
weekly_collection_name = "Weekly:" + today.year.to_s + today.month.to_s + today.day.to_s
7.times do |i|
  d = today - i
  collection_name = "Daily:" + d.year.to_s + d.month.to_s + d.day.to_s
  coll = db.collection(collection_name)
  # dont run map_reduce on collections that dont exist
  coll.map_reduce(map, reduce, { :out => {:reduce => weekly_collection_name} }) if coll.count > 0
end

####################################################
# send number of weekly active users to StatsD
####################################################
coll = db.collection(weekly_collection_name)
statsd = Statsd.new(STATSD_SERVER, STATSD_PORT)
statsd.count('activity.weekly.total', coll.count)
statsd.count('activity.weekly.one_visit', coll.find("value.days_with_activity"=>{"$gt"=>2}).count)