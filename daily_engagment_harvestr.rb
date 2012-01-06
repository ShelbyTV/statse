#!/usr/bin/env ruby
# encoding: UTF-8

# Run Frequency: Daily @ 12:01 AM
# This script harvests data from mongodb and sends it to a statsd server

require_relative 'config'

db = Mongo::Connection.new(MONGO_HOST, MONGO_PORT).db(DB_NAME)

####################################################
# definiton of actions
#  {"action_name" (string) => multiplier (int) }
#
# ** note: NOT including ios sharing actions because 
#       they are also reported by the rails backend 
####################################################
THRESHOLD_RANGE = 20 # allows us to pick what the threshold is after the fact.

actions = {
  "twitter_signin" => 1,
  "facebook_signin" => 1,
  "watch" => 3,
  "like" => 4,
  "watch_later" => 4,
  "bookmarklet" => 5,
  "add_twitter" => 5,
  "add_facebook" => 5,
  "add_tumblr" => 5,
  "twitter_post" => 6,
  "facebook_post" => 6,
  "tumblr_post" => 6,
  "send_email" => 6,
  # ios actions:
    # iphone
  "ios_iphone_signin" => 1,
  "ios_iphone_watch" => 3,
  "ios_iphone_like" => 4,
  "ios_iphone_watch_later" => 4,
    # iphone
  "ios_ipad_signin" => 1,
  "ios_ipad_watch" => 3,
  "ios_ipad_like" => 4,
  "ios_ipad_watch_later" => 4
}

####################################################
# get yesterdays daily activity collection name,
#  loop through it adding each document to a new collection,
#   "DailyEngagement:20120104", while multiplying each value
#     by its score (set in the activity hash).
####################################################
yesterday = Date.today - 1
dau_coll = db.collection("Daily:" + yesterday.strftime("%Y%m%d"))
deu_coll = db.collection("DailyEngagement:" + yesterday.strftime("%Y%m%d"))

total_engagement = 0
engaged_users = 0
THRESHOLD_RANGE.times do |i|
  var_name = "@engaged_users_" + (i + 4).to_s
  instance_variable_set(var_name, 0)
end


dau_coll.find.each do |user|
  doc = {}
  sum = 0
  actions.keys.each do |action|
    user.keys.each do |key|
      case key
      when "_id"
        doc[key] = user[key]
      when action
        doc["activity"] = { key => actions[action] * user[key]}
      end
    end
  end
  doc["activity"].values.each {|x| sum += x if x.class == Fixnum} if doc["activity"]
  doc["engagement"] = sum
  THRESHOLD_RANGE.times do |i|
    var_name = "@engaged_users_" + (i + 4).to_s
    instance_variable_set(var_name, instance_variable_get(var_name)+1) if sum >= i + 4
  end
  total_engagement += sum
  deu_coll.insert(doc)
end

####################################################
# send engagement stats to StatsD
####################################################
statsd = Statsd.new(STATSD_SERVER, STATSD_PORT)
engagement_mean = total_engagement.to_f / dau_coll.count.to_f
statsd.count('engagement.daily.mean', engagement_mean)
THRESHOLD_RANGE.times do |i|
  var_name = "@engaged_users_" + (i + 4).to_s
  statsd.count('engagement.daily.threshold.' + (i+4).to_s, instance_variable_get(var_name) )  
end

