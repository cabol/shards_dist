%%%-------------------------------------------------------------------
%%% @doc
%%% Main supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist_sup).

-behavior(supervisor).

%% API
-export([
  start_link/0
]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, undefined).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init(_Arg) ->
  {module, shards_dist_pg} = code:ensure_loaded(shards_dist_pg),

  Children =
    case erlang:function_exported(shards_dist_pg, child_spec, 0) of
      true  -> [shards_dist_pg:child_spec()];
      false -> []
    end,

  {ok, {{one_for_one, 10, 10}, Children}}.
