================
Erlang-Beanstalk
================


What is this?
-------------

An Erlang module that wraps the beanstalkd protocol.


What is beanstalkd?
-------------------

A fast, distributed, in-memory workqueue service.

See http://xph.us/software/beanstalkd/ for more information,
and links to clients in other languages.


How do I use it?
----------------

First, make sure both of the modules are compiled, and that you
have an instance of beanstalkd running in the background. Then
you can connect to it like so (using whatever host and port values
are appropriate for you):

  {ok, Socket} = beanstalk:connect(_Host="0.0.0.0", _Port=3000).

At the moment this is just a direct call to gen_tcp:connect/3.

Jobs are manipulated using the beanstalk_job module. To create
a new job, pass a string or a binary to beanstalk_job:new/1:

  Job = beanstalk_job:new("hello").

You can alter the defaults using beanstalk_job:with/3, e.g.,

  DelayedJob = beanstalk_job:with(delay, 30, Job).

All the functions in the beanstalk module (apart from connect/2)
correspond to calls in the beanstalkd protocol. Each expects to
be given the socket connection as the last argument. For example:

  {inserted, JobID} = beanstalk:put(Job, Socket).

  {reserved, Job} = beanstalk:reserve(Socket).

  deleted = beanstalk:delete(Job, Socket).

And so on.


WTF?
----

Found a bug? Think I could have done something better?

Don't let it trouble you :) Let me know!
