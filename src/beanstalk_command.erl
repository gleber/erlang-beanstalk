-module(beanstalk_command).

-export([test/0, send/2, send/3, to_binary/1]).


test() ->
  <<"reserve\r\n">> = to_binary({reserve}),
  <<"use foo\r\n">> = to_binary({use, "foo"}),
  ok.

send(Socket, Command) ->
  gen_tcp:send(Socket, to_binary(Command)).

send(Socket, Command, Data) ->
  gen_tcp:send(Socket, to_binary(Command)),
  gen_tcp:send(Socket, Data),
  gen_tcp:send(Socket, "\r\n").

to_binary(Cmd) when is_tuple(Cmd) ->
  to_binary(tuple_to_list(Cmd));
to_binary(Cmd) when is_list(Cmd) ->
  to_binary(Cmd, []).

to_binary([], Acc) ->
  iolist_to_binary(lists:reverse(["\r\n"|Acc]));
to_binary([H | T], []) ->
  to_binary(T, [to_string(H)]);
to_binary([H | T], Acc) ->
  to_binary(T, [to_string(H), 32 | Acc]).

to_string(Term) when is_integer(Term) ->
  integer_to_list(Term);
to_string(Term) when is_atom(Term) ->
  atom_to_list(Term);
to_string(Term) when is_list(Term) ->
  Term.
