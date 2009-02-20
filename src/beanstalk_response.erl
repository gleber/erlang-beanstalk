-module(beanstalk_response).

-export([parse/1, recv/1]).


recv(Socket) ->
  case gen_tcp:recv(Socket, 0) of
    {ok, Packet} ->
      parse(Packet);
    Error ->
      Error
  end.

parse(<<"OUT_OF_MEMORY\r\n">>) ->
  {out_of_memory};
parse(<<"INTERNAL_ERROR\r\n">>) ->
  {internal_error};
parse(<<"DRAINING\r\n">>) ->
  {draining};
parse(<<"BAD_FORMAT\r\n">>) ->
  {bad_format};
parse(<<"UNKNOWN_COMMAND\r\n">>) ->
  {unknown_command};
parse(<<"EXPECTED_CRLF\r\n">>) ->
  {expected_crlf};
parse(<<"JOB_TOO_BIG\r\n">>) ->
  {job_too_big};
parse(<<"DEADLINE_SOON\r\n">>) ->
  {deadline_soon};
parse(<<"TIMED_OUT\r\n">>) ->
  {timed_out};
parse(<<"DELETED\r\n">>) ->
  {deleted};
parse(<<"NOT_FOUND\r\n">>) ->
  {not_found};
parse(<<"RELEASED\r\n">>) ->
  {released};
parse(<<"BURIED\r\n">>) ->
  {buried};
parse(<<"TOUCHED\r\n">>) ->
  {touched};
parse(<<"NOT_IGNORED\r\n">>) ->
  {not_ignored};
parse(<<"INSERTED ", Bin/bytes>>) ->
  parse_int(Bin, inserted);
parse(<<"BURIED ", Bin/bytes>>) ->
  parse_int(Bin, buried);
parse(<<"WATCHING ", Bin/bytes>>) ->
  parse_int(Bin, watching);
parse(<<"KICKED ", Bin/bytes>>) ->
  parse_int(Bin, kicked);
parse(<<"USING ", Bin/bytes>>) ->
  parse_string(Bin, using);
parse(<<"RESERVED ", Bin/bytes>>) ->
  {ID, <<" ", Bin2/bytes>>} = parse_digits(Bin),
  {Bytes, <<"\r\n", Bin3/bytes>>} = parse_digits(Bin2),
  {Data, <<"\r\n">>} = split_binary(Bin3, Bytes),
  {reserved, ID, Data};
parse(<<"FOUND ", Bin/bytes>>) ->
  {ID, <<" ", Bin2/bytes>>} = parse_digits(Bin),
  {Bytes, <<"\r\n", Bin3/bytes>>} = parse_digits(Bin2),
  {Data, <<"\r\n">>} = split_binary(Bin3, Bytes),
  {found, ID, Data};
parse(<<"OK ", Bin/bytes>>) ->
  {Bytes, <<"\r\n", Bin2/bytes>>} = parse_digits(Bin),
  {Data, <<"\r\n">>} = split_binary(Bin2, Bytes),
  {ok, beanstalk_yaml:parse(Data)}.

parse_string(Bin, Name) ->
  parse_string(Bin, Name, []).

parse_string(<<"\r\n">>, Name, Acc) ->
  {Name, lists:reverse(Acc)};
parse_string(<<C, Bin/bytes>>, Name, Acc) ->
  parse_string(Bin, Name, [C | Acc]).

parse_int(Bin, Name) ->
  {Int, <<"\r\n">>} = parse_digits(Bin),
  {Name, Int}.

parse_digits(Bin) ->
  parse_digits(Bin, []).

parse_digits(Bin, Acc) ->
  case Bin of
    <<C, Bin2/bytes>> when C >= $0 andalso C =< $9 ->
      parse_digits(Bin2, [C | Acc]);
    _ ->
      {list_to_integer(lists:reverse(Acc)), Bin}
  end.
