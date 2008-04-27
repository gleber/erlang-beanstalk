% Copyright (c) 2008 Tim Fletcher <tfletcher.com>
% 
% Permission is hereby granted, free of charge, to any person
% obtaining a copy of this software and associated documentation
% files (the "Software"), to deal in the Software without
% restriction, including without limitation the rights to use,
% copy, modify, merge, publish, distribute, sublicense, and/or sell
% copies of the Software, and to permit persons to whom the
% Software is furnished to do so, subject to the following
% conditions:
% 
% The above copyright notice and this permission notice shall be
% included in all copies or substantial portions of the Software.
% 
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
% OTHER DEALINGS IN THE SOFTWARE.

-module(beanstalk_job).

-export([new/1]).
-export([new/2]).
-export([id/1]).
-export([body/1]).
-export([priority/1]).
-export([delay/1]).
-export([ttr/1]).
-export([with/3]).


new(ID) when is_integer(ID) ->
  [{id, ID}];
new(Body) when is_list(Body); is_binary(Body) ->
  [{body, Body}].

new(ID, Body) ->
  [{id, ID}, {body, Body}].

id(Job) ->
  proplists:get_value(id, Job).

body(Job) ->
  proplists:get_value(body, Job).

priority(Job) ->
  proplists:get_value(priority, Job, 0).

delay(Job) ->
  proplists:get_value(delay, Job, 0).

ttr(Job) ->
  proplists:get_value(ttr, Job, 60).

with(Key, Value, Job) ->
  [{Key, Value}|proplists:delete(Key, Job)].
