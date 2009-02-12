-module(beanstalk).
-behaviour(gen_server2).

% gen_server callbacks
-export(
  [init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export(
  [connect/0, connect/1, connect/2

  ,put/1, put/2
  ,use/1
  ,reserve/0
  ,reserve_with_timeout/0, reserve_with_timeout/1
  ,delete/1
  ,release/1, release/2
  ,bury/1, bury/2
  ,touch/1
  ,watch/1
  ,ignore/1
  ,peek/1
  ,peek_ready/0
  ,peek_delayed/0
  ,peek_buried/0
  ,kick/1
  ,stats_job/1
  ,stats_tube/1
  ,stats/0
  ,list_tubes/0
  ,list_tube_used/0
  ,list_tubes_watched/0
  ]).

-define(TIMEOUT_START, 30000).
-define(TIMEOUT_CLIENT, 15000).
-define(TIMEOUT_SERVER, 60000).
-define(DEFAULT_HOST, {127,0,0,1}).
-define(DEFAULT_PORT, 11300).

% gen_server callbacks
init([Host, Port]) ->
  Me = self(),
  Pid = spawn(fun() -> start_(Me, Host, Port) end),
  receive
  {ok, Pid} ->
    {ok, Pid};
  {error, Reason} ->
    {stop, Reason}
  after ?TIMEOUT_START->
    {stop, {timeout, ?TIMEOUT_START}}
  end.

handle_call(Msg, _From, State) ->
  State ! {Msg, self()},
  receive
    Reply -> {reply, Reply, State}
  end.

handle_cast(Msg, State) when is_tuple(Msg) ->
  State ! Msg,
  {noreply, State};
handle_cast(Msg, State) when is_atom(Msg) ->
  State ! {Msg},
  {noreply, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({'EXIT', normal}, _State) ->
  {stop, normal, exited};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  State ! close.

code_change(_OldVersion, State, _Extra) ->
  {ok, State}.

% internal functions for gen_server

start_(Parent, Host, Port) ->
  case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}]) of
    {ok, Socket} -> Parent ! {ok, self()}, loop(Socket);
    Other -> Parent ! Other
  end.

loop(Socket) ->
  case
    receive
    {{send, Body}, Who} ->
      case gen_tcp:send(Socket, Body) of
        ok ->
          R = from_socket(Socket, <<>>),
          Who ! R,
          R;
        R ->
          Who ! R,
          R
      end;
    {answer, Who} ->
      R = from_socket(Socket, <<>>),
      Who ! R,
      R;
    {close, _} ->
      gen_tcp:close(Socket),
      {error, closed}
    end
  of
    {ok, _} -> loop(Socket);
    Error -> Error
  end.

from_socket(Socket, Data) ->
  case gen_tcp:recv(Socket, 0) of
    {ok, Packet} ->
      case size(Packet) - 2 of
        S when S < 0 ->
          from_socket(Socket, <<Data/binary, Packet/binary>>);
        L ->
          case Packet of
            << Stripped:L/binary, $\r, $\n >> ->
              {ok, <<Data/binary, Stripped/binary>>};
            _ ->
              from_socket(Socket, <<Data/binary, Packet/binary>>)
          end
      end;
    Error ->
      case Error of
        {error, closed} -> ok;
        _ -> gen_tcp:close(Socket)
      end,
      Error
  end.

% external API

connect() -> connect(?DEFAULT_HOST).
connect(IP) -> connect(IP, ?DEFAULT_PORT).
connect(Host, Port) ->
  gen_server2:start_link({local, ?MODULE}, ?MODULE, [Host, Port], []).

put(Item) -> ?MODULE:put(Item, []).

put(Data, PL) when is_integer(Data) -> ?MODULE:put(integer_to_list(Data), PL);
put(Body, PL) when is_list(Body); is_binary(Body) ->
  P = fun(Key, Default) -> Value = proplists:get_value(Key, PL, Default), true = is_integer(Value), Value  end,
  Response = send_command({put, P(pri, 0), P(delay, 0), P(ttr, 60), size_of(Body)}, Body),
  process('job_too_big',
  process('exptected_clrf',
  process_int(buried,
  process_int(inserted, process_response(Response))))).

use(Tube) ->
  Response = send_command({use, Tube}),
  process_using(process_response(Response)).

reserve() ->
  Response = send_and_wait_command(reserve),
  process(deadline_soon,
  process_job(reserved, process_response(Response))).

reserve_with_timeout() -> reserve_with_timeout(0).
reserve_with_timeout(Timeout) when is_integer(Timeout), Timeout >= 0 ->
  Response = send_and_wait_command({'reserve-with-timeout', Timeout}),
  process(timed_out,
  process(deadline_soon,
  process_job(reserved, process_response(Response)))).

delete(ID) when is_integer(ID) ->
  Response = send_command({delete, ID}),
  process(deleted,
  process_not_found(process_response(Response))).

release(Job) ->
  release(Job, []).

release(Job, PL) when is_integer(Job), is_list(PL) ->
  P = fun(Key, Default) -> Value = proplists:get_value(Key, PL, Default), true = is_integer(Value), Value  end,
  Response = send_command({release, Job, P(pri, 0), P(delay, 0)}),
  process(released,
  process_buried(process_not_found(process_response(Response)))).

bury(Job) ->
  bury(Job, 0).

bury(Job, Pri) when is_integer(Job), is_integer(Pri), pri >= 0 ->
  Response = send_command({bury, Job, Pri}),
  process_buried(process_not_found(process_response(Response))).

touch(Job) when is_integer(Job) ->
  Response = send_command({touch, Job}),
  process(touched, process_not_found(process_response(Response))).

watch(Tube) ->
  Response = send_command({watch, Tube}),
  process_watching(process_response(Response)).

ignore(Tube) ->
  Response = send_command({ignore, Tube}),
  process(not_ignored,
  process_watching(process_response(Response))).

peek(ID) when is_integer(ID) ->
  Response = send_command({peek, ID}),
  receive_peek_response(Response).

peek_ready() ->
  Response = send_command('peek-ready'),
  receive_peek_response(Response).

peek_delayed() ->
  Response = send_command('peek-delayed'),
  receive_peek_response(Response).

peek_buried() ->
  Response = send_command('peek-buried'),
  receive_peek_response(Response).

kick(Bound) when is_integer(Bound) ->
  Response = send_command({kick, Bound}),
  process_int(kicked, process_response(Response)).

stats_job(ID) ->
  Response = send_command({'stats-job', ID}),
  process_yaml(process_not_found(process_response(Response))).

stats_tube(Tube) ->
  Response = send_command({'stats-tube', Tube}),
  process_yaml(process_not_found(process_response(Response))).

stats() ->
  Response = send_command(stats),
  process_yaml(process_response(Response)).

list_tubes() ->
  Response = send_command('list-tubes'),
  process_yaml(process_response(Response)).

list_tube_used() ->
  Response = send_command('list-tube-used'),
  process_using(process_response(Response)).

list_tubes_watched() ->
  Response = send_command('list-tubes-watched'),
  process_yaml(process_response(Response)).

receive_peek_response(Response) ->
  process_job(found, process_not_found(process_response(Response))).

process_watching(Response) ->
  process_int(watching, Response).

process_buried(Response) ->
  process(buried, Response).

process_not_found(Response) ->
  process(not_found, Response).

process_using(Response) ->
  process_prefixed(using, fun binary_to_list/1, Response).

process_yaml({ok, <<"OK ", Bin/bytes>>}) ->
  {DataLength, <<"\r\n", Rem/bytes>>} = binary_take_int(Bin),
  {ok, yaml_parse(element(1, split_binary(Rem, DataLength)))};
process_yaml(Response) ->
  Response.

process_job(Atom, Response) ->
  process_prefixed(Atom, fun process_job_data/1, Response).

process_job_data(Bin) ->
  {ID, <<" ", Bin2/bytes>>} = binary_take_int(Bin),
  {_BodyLength, <<"\r\n", Body/bytes>>} = binary_take_int(Bin2),
  {ID, Body}.

process_int(Atom, Response) ->
  process_prefixed(Atom, fun binary_to_integer/1, Response).

process_prefixed(Atom, Fun, Response={ok, Data}) ->
  Prefix = iolist_to_binary([string:to_upper(atom_to_list(Atom)), $ ]),
  case split_binary(Data, size(Prefix)) of
    {Prefix, Rest} ->
      {Atom, Fun(Rest)};
    _ ->
      Response
  end;
process_prefixed(_Atom, _Fun, Response) ->
  Response.

process_response(Response) ->
  case
      process(out_of_memory,
      process(internal_error,
      process(draining,
      process(bad_format,
      process(unknown_command,
      Response))))) of
    Known when is_atom(Known) -> {error, Known};
    _ -> Response
  end.

process(Term, Response={ok, Message}) ->
  case list_to_binary(string:to_upper(atom_to_list(Term))) of
    Message -> Term;
    _ -> Response
  end;
process(_, Response) ->
    Response.

binary_to_integer(Bin) when is_binary(Bin) ->
  list_to_integer(binary_to_list(Bin)).

binary_take_int(Bin) when is_binary(Bin) ->
  binary_take_int(Bin, []).

binary_take_int(<<C, Rem/bytes>>, Digits) when C >= $0, C =< $9 ->
  binary_take_int(Rem, [C|Digits]);
binary_take_int(Bin, Digits) ->
  {list_to_integer(lists:reverse(Digits)), Bin}.

send_command(Cmd) when is_binary(Cmd); is_list(Cmd) ->
  gen_server2:call(?MODULE, {send, Cmd});
send_command(Cmd) ->
  send_command(build_command(case is_atom(Cmd) of true -> {Cmd}; _ -> Cmd end)).

send_command(Cmd, Data) ->
  gen_server2:call(?MODULE, {send, iolist_to_binary([build_command(Cmd), Data, "\r\n"])}).

send_and_wait_command(Cmd) when is_binary(Cmd); is_list(Cmd) ->
  gen_server2:cast(?MODULE, {{send, Cmd}, self()}),
  receive
    Reply -> Reply
  end;
send_and_wait_command(Cmd) ->
  send_and_wait_command(build_command(case is_atom(Cmd) of true -> {Cmd}; _ -> Cmd end)).

build_command(Cmd) when is_tuple(Cmd) ->
  build_command(tuple_to_list(Cmd), []).

build_command([], Parts) -> iolist_to_binary([lists:reverse(Parts) | "\r\n"]);
build_command([H|T], Parts) ->
  build_command(T, [to_string(H) | case Parts of [] -> []; _ -> [$ | Parts] end]).

to_string(Int) when is_integer(Int) ->
  integer_to_list(Int);
to_string(Atom) when is_atom(Atom) ->
  atom_to_list(Atom);
to_string(List) ->
  List.

size_of(List) when is_list(List) ->
  length(List);
size_of(Bin) when is_binary(Bin) ->
  size(Bin).

yaml_parse(<<"---\n", Data/bytes>>) ->
  case Data of
    <<"- ", _/bytes>> ->
      yaml_parse_sequence(Data, []);
    _ ->
      yaml_parse_mapping(Data, [])
  end.

yaml_parse_sequence(Data, Sequence) when size(Data) =:= 0 ->
  lists:reverse(Sequence);
yaml_parse_sequence(<<"- ", Data/bytes>>, Sequence) ->
  {Value, MoreData} = binary_break_at($\n, Data),
  yaml_parse_sequence(MoreData, [Value|Sequence]).

yaml_parse_mapping(Data, Mapping) when size(Data) =:= 0 ->
  Mapping;
yaml_parse_mapping(Data, Mapping) ->
  {K, <<" ", Rest/bytes>>} = binary_break_at($:, Data),
  {V, MoreData} = binary_break_at($\n, Rest),
  yaml_parse_mapping(MoreData, [{K,V}|Mapping]).

binary_break_at(C, Data) when is_binary(Data) ->
  binary_break_at(C, Data, []).

binary_break_at(_C, Data, First) when is_binary(Data), size(Data) =:= 0 ->
  {lists:reverse(First), []};
binary_break_at(C, Data, First) when is_binary(Data) ->
  <<Head, Tail/bytes>> = Data,
  case Head of
    C ->
      {lists:reverse(First), Tail};
    _ ->
      binary_break_at(C, Tail, [Head|First])
  end.
