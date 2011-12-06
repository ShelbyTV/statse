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
        emit(this._id, {days_with_activity: 1});
      }
      JS

reduce = <<-JS
      function (key, values) {
        var sum = 0;
        values.forEach(function (f) {
          sum += f.days_with_activity;
        });
        return {days_with_activity: sum};
      } 
      JS

####################################################
# get collection names of past 7 days
#  and run M/R on each collection
#  output goes to one collection, weekly:date
####################################################
today = Date.today - 1
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
weekly_coll = db.collection(weekly_collection_name)
statsd = Statsd.new(STATSD_SERVER, STATSD_PORT)
statsd.count('activity.weekly.total', weekly_coll.count)
7.times do |i|
  i+=1
  statsd.count('activity.weekly.visits.#{i}', weekly_coll.find("value.days_with_activity" => i).count)  
end

####################################################
# calculate retention rate and send to StatsD
####################################################
day_1 = Date.today - 1
day_2 = Date.today - 8
collecttion_name_a = "Weekly:" + day_1.year.to_s + day_1.month.to_s + day_1.day.to_s
collection_name_b = "Weekly:" + day_2.year.to_s + day_2.month.to_s + day_2.day.to_s
collection_a = db.collection(collection_name_a)
collection_b = db.collection(collection_name_b)

# run map_reduce over both weeks collections and only keep those with value.count > 1 to get the logical AND
collection_a.map_reduce(map, reduce, { :out => {:reduce => "retention:temp"} }) if collection_a.count > 0
collection_b.map_reduce(map, reduce, { :out => {:reduce => "retention:temp"} }) if collection_b.count > 0

temp_collection = db.collection("retention:temp")
temp_collection_size = temp_collection.count
raw_retention = temp_collection.find( "value.days_with_activity" => {"$gt" => 1} ).count
temp_collection.drop() # this was only temporary, say goodbye

# now compute actual retention over period and send to statsd
retention = (raw_retention.to_f / temp_collection_size.to_f) * 100
statsd.count('activity.retention.weekly', retention.to_i)