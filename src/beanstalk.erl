-module(beanstalk).

-compile(export_all).


connect() ->
  connect({127, 0, 0, 1}).

connect(Host) ->
  connect(Host, 11300).

connect(Host, Port) ->
  connect(Host, Port, 5000).

connect(Host, Port, Timeout) ->
  gen_server:start_link(beanstalk_connection, {Host, Port, Timeout}, []).

close(Pid) ->
  gen_server:cast(Pid, {'stop'}).

put(Pid, Data) ->
  put(Pid, Data, []).

put(Pid, Data, Params) ->
  gen_server:call(Pid, {'put', Data, Params}).

use(Pid, Tube) ->
  gen_server:call(Pid, {'use', Tube}).

reserve(Pid) ->
  gen_server:call(Pid, {'reserve'}, infinity).

reserve(Pid, Timeout) ->
  gen_server:call(Pid, {'reserve-with-timeout', Timeout}, infinity).

delete(Pid, ID) ->
  gen_server:call(Pid, {'delete', ID}).

release(Pid, ID) ->
  release(Pid, ID, []).

release(Pid, ID, Params) ->
  gen_server:call(Pid, {'release', ID, Params}).

bury(Pid, ID) ->
  bury(Pid, ID, 0).

bury(Pid, ID, Priority) ->
  gen_server:call(Pid, {'bury', ID, Priority}).

touch(Pid, ID) ->
  gen_server:call(Pid, {'touch', ID}).

watch(Pid, Tube) ->
  gen_server:call(Pid, {'watch', Tube}).

ignore(Pid, Tube) ->
  gen_server:call(Pid, {'ignore', Tube}).

peek(Pid, ID) ->
  gen_server:call(Pid, {'peek', ID}).

peek_ready(Pid) ->
  gen_server:call(Pid, {'peek-ready'}).

peek_delayed(Pid) ->
  gen_server:call(Pid, {'peek-delayed'}).

peek_buried(Pid) ->
  gen_server:call(Pid, {'peek-buried'}).

kick(Pid, Bound) ->
  gen_server:call(Pid, {'kick', Bound}).

stats_job(Pid, ID) ->
  gen_server:call(Pid, {'stats-job', ID}).

stats_tube(Pid, Tube) ->
  gen_server:call(Pid, {'stats-tube', Tube}).

stats(Pid) ->
  gen_server:call(Pid, {'stats'}).

list_tubes(Pid) ->
  gen_server:call(Pid, {'list-tubes'}).

list_tube_used(Pid) ->
  gen_server:call(Pid, {'list-tube-used'}).

list_tubes_watched(Pid) ->
  gen_server:call(Pid, {'list-tubes-watched'}).
