-module(beanstalk_response).

-export([parse/1, recv/1, recv/2]).

recv(Socket) ->
  recv(Socket, <<>>).
recv(Socket, Data) ->
  case gen_tcp:recv(Socket, 0) of
    {ok, Packet} ->
      NewData = <<Data/binary, Packet/binary>>,
      case parse(NewData) of
        more ->
          recv(Socket, NewData);
        {ok, X, Rest} ->
          {ok, X, Rest}
      end;
    Error ->
      Error
  end.

parse(<<"OUT_OF_MEMORY\r\n", Rest/binary>>) ->
  {ok, {out_of_memory}, Rest};
parse(<<"INTERNAL_ERROR\r\n", Rest/binary>>) ->
  {ok, {internal_error}, Rest};
parse(<<"DRAINING\r\n", Rest/binary>>) ->
  {ok, {draining}, Rest};
parse(<<"BAD_FORMAT\r\n", Rest/binary>>) ->
  {ok, {bad_format}, Rest};
parse(<<"UNKNOWN_COMMAND\r\n", Rest/binary>>) ->
  {ok, {unknown_command}, Rest};
parse(<<"EXPECTED_CRLF\r\n", Rest/binary>>) ->
  {ok, {expected_crlf}, Rest};
parse(<<"JOB_TOO_BIG\r\n", Rest/binary>>) ->
  {ok, {job_too_big}, Rest};
parse(<<"DEADLINE_SOON\r\n", Rest/binary>>) ->
  {ok, {deadline_soon}, Rest};
parse(<<"TIMED_OUT\r\n", Rest/binary>>) ->
  {ok, {timed_out}, Rest};
parse(<<"DELETED\r\n", Rest/binary>>) ->
  {ok, {deleted}, Rest};
parse(<<"NOT_FOUND\r\n", Rest/binary>>) ->
  {ok, {not_found}, Rest};
parse(<<"RELEASED\r\n", Rest/binary>>) ->
  {ok, {released}, Rest};
parse(<<"BURIED\r\n", Rest/binary>>) ->
  {ok, {buried}, Rest};
parse(<<"TOUCHED\r\n", Rest/binary>>) ->
  {ok, {touched}, Rest};
parse(<<"NOT_IGNORED\r\n", Rest/binary>>) ->
  {ok, {not_ignored}, Rest};
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
  parse_job(Bin, reserved);
parse(<<"FOUND ", Bin/bytes>>) ->
  parse_job(Bin, found);
parse(<<"OK ", Bin/bytes>>) ->
  case parse_body(Bin) of
    {ok, Body, Rest} ->
      {ok, beanstalk_yaml:parse(Body), Rest};
    more ->
      more
  end;
parse(_) ->
  more.

parse_job(Bin, Name) ->
  case parse_id(Bin) of
    {ok, ID, Bin2} ->
      case parse_body(Bin2) of
        {ok, Body, Rest} ->
          {ok, {Name, ID, Body}, Rest};
        more ->
          more
      end;
    more ->
      more
  end.

parse_id(Bin) ->
  case parse_digits(Bin) of
    {ok, Id, <<" ", Rest/binary>>} ->
      {ok, Id, Rest};
    _ ->
      more
  end.

parse_body(Bin) ->
  case parse_digits(Bin) of
    {ok, Length, <<"\r\n", Rest/binary>>} ->
      case Rest of
        <<Body:Length/binary, "\r\n", Rest2/binary>> ->
          {ok, Body, Rest2};
        _ ->
          more
      end;
    _ ->
      more
  end.

parse_string(Bin, Name) ->
  parse_string(Bin, Name, []).

parse_string(<<>>, _, _) ->
  more;
parse_string(<<"\r\n", Rest/binary>>, Name, Acc) ->
  {ok, {Name, lists:reverse(Acc)}, Rest};
parse_string(<<C, Bin/bytes>>, Name, Acc) ->
  parse_string(Bin, Name, [C | Acc]).

parse_int(Bin, Name) ->
  case parse_digits(Bin) of
    {ok, Int, <<"\r\n", Rest/binary>>} ->
      {ok, {Name, Int}, Rest};
    _ ->
      more
  end.

parse_digits(Bin) ->
  parse_digits(Bin, []).

parse_digits(Bin, Acc) ->
  case Bin of
    <<C, Bin2/bytes>> when C >= $0 andalso C =< $9 ->
      parse_digits(Bin2, [C | Acc]);
    _ ->
      {ok, list_to_integer(lists:reverse(Acc)), Bin}
  end.
