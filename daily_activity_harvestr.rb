#!/usr/bin/env ruby
# encoding: UTF-8

# Run Frequency: Every 10min
# This script harvests data from mongodb and sends it to a statsd server

require_relative 'config'

#####################
# get count of todays DAU
#####################
db = Mongo::Connection.new(MONGO_HOST, MONGO_PORT).db(DB_NAME)

today = Date.today
collection_name = "Daily:" + today.strftime("%Y%m%d")
coll = db.collection(collection_name)
dau = coll.count()

#####################
# send stat to statsd 
#####################
statsd = Statsd.new(STATSD_SERVER, STATSD_PORT)
statsd.count('activity.daily', dau)