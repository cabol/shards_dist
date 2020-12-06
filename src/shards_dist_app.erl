%%%-------------------------------------------------------------------
%%% @doc
%%% Application.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist_app).

-behaviour(application).

%% Application callbacks
-export([
  start/2,
  stop/1
]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%% @hidden
start(_StartType, _StartArgs) ->
  shards_dist_sup:start_link().

%% @hidden
stop(_State) ->
  ok.
