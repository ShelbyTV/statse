#!/usr/bin/env ruby
# encoding: UTF-8

# This script harvests data from a mongo db and sends it to a statsd server

require_relative 'config.rb'

#####################
# get count of todays DAU
#####################
db = Mongo::Connection.new(mongo_host, mongo_port).db(db_name)

today = Date.today
collection_name = "Daily:" + today.year.to_s + today.month.to_s + today.day.to_s
coll = db.collection(collection_name)
dau = coll.count()

#####################
# send stat to statsd 
#####################
statsd = Statsd.new(statsd_server, statsd_port)
statsd.count('activity.daily', dau)