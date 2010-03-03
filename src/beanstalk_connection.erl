-module(beanstalk_connection).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, buffer = <<>>}).

init({Host, Port, Timeout}) ->
  case gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}], Timeout) of
    {ok, Socket} ->
      {ok, #state{socket = Socket}};
    Error ->
      {stop, Error}
  end.

handle_call({put, Data, Params}, _From, #state{socket = Socket, buffer = Buffer} = State) ->
  Pri = proplists:get_value(pri, Params, 0),
  Delay = proplists:get_value(delay, Params, 0),
  TTR = proplists:get_value(ttr, Params, 60),
  beanstalk_command:send(Socket, {put, Pri, Delay, TTR, size(Data)}, Data),
  {ok, Reply, <<>> = NewBuffer} = beanstalk_response:recv(Socket, Buffer),
  {reply, Reply, State#state{buffer = NewBuffer}};
handle_call(Command, _From, #state{socket = Socket, buffer = Buffer} = State) ->
  beanstalk_command:send(Socket, Command),
  {ok, Reply, <<>> = NewBuffer} = beanstalk_response:recv(Socket, Buffer),
  {reply, Reply, State#state{buffer = NewBuffer}}.

handle_cast({stop}, State) ->
  {stop, normal, State};
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Msg, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(normal, Socket) ->
  catch gen_tcp:close(Socket),
  ok.
