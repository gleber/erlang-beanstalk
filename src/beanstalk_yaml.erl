-module(beanstalk_yaml).

-export([parse/1, test/0]).


test() ->
  ["one", "two"] = parse(<<"---\n- one\n- two\n">>),
  [{"one", "1"}, {"two", "2"}] = parse(<<"---\none: 1\ntwo: 2\n">>),
  {"hello", <<"world">>} = binary_break_at($\n, <<"hello\nworld">>).

parse(<<"---\n", Data/bytes>>) ->
  parse_document(Data);
parse(Data) ->
  parse_document(Data).

parse_document(Data) ->
  case Data of
    <<"- ", _/bytes>> ->
      parse_sequence(Data, []);
    _ ->
      parse_mapping(Data, [])
  end.

parse_sequence(<<>>, Sequence) ->
  lists:reverse(Sequence);
parse_sequence(<<"- ", Data/bytes>>, Sequence) ->
  {Value, MoreData} = binary_break_at($\n, Data),
  parse_sequence(MoreData, [Value|Sequence]).

parse_mapping(<<>>, Mapping) ->
  lists:reverse(Mapping);
parse_mapping(Data, Mapping) ->
  {K, <<" ", Rest/bytes>>} = binary_break_at($:, Data),
  {V, MoreData} = binary_break_at($\n, Rest),
  parse_mapping(MoreData, [{K, V}|Mapping]).

binary_break_at(C, Data) when is_binary(Data) ->
  binary_break_at(C, Data, []).

binary_break_at(_C, <<>>, Prefix) ->
  {lists:reverse(Prefix), []};
binary_break_at(C, Data, Prefix) when is_binary(Data) ->
  <<H, T/bytes>> = Data,
  case H of
    C ->
      {lists:reverse(Prefix), T};
    _ ->
      binary_break_at(C, T, [H | Prefix])
  end.
