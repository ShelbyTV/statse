StatsE
======

An aggregator of data from MongoDB that passing information to [graphite][graphite] and [statsd][statsd].

We ([Etsy][etsy]) [blogged][blog post] about how it works and why we created it.

Pimped for Shelby
--------
Stats sent via UDP to our statsd/graphite server will work as before and here is what is newly added:

Suppose we have a namespaced stat like *'stats.app.broadcast.watch'*
we can now add parameters to that namespace that include a `UID` and an `action` (delimited by an '&' keeping with a url type structure).

eg: `'stats.app.broadcast.watch/?uid=123456&action=watch'`

When stats of this nature are picked up by the Statsd node process, a document is either *inserted or updated* in a Mongo collection (that is date specific).  This will allow us to get a count of "active" users for every given day AND has the added feature of storing the actions that each user takes on each day

Here is the list of actions that I have coded into the web app:

* *signin*

* *watch*
