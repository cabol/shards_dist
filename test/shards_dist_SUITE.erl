-module(shards_dist_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Tests Cases
-export([
  t_join_leave_ops/1
]).

-define(SLAVES, [
  'a@127.0.0.1',
  'b@127.0.0.1',
  'c@127.0.0.1',
  'd@127.0.0.1',
  'e@127.0.0.1'
]).

%%%===================================================================
%%% Common Test
%%%===================================================================

-spec all() -> [{group, atom()}].
all() ->
  [{group, dist_test_group}].

-spec groups() -> [any()].
groups() ->
  [
    {dist_test_group, [sequence], [
      t_join_leave_ops
    ]}
  ].

-spec init_per_suite(shards_ct:config()) -> shards_ct:config().
init_per_suite(Config) ->
  {ok, Nodes} = shards_dist_cluster:start(?SLAVES),
  ok = application:start(shards_dist),
  [{nodes, Nodes} | Config].

-spec end_per_suite(shards_ct:config()) -> shards_ct:config().
end_per_suite(Config) ->
  ok = application:stop(shards_dist),
  _ = shards_dist_cluster:stop(?SLAVES),
  Config.

-spec init_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
init_per_testcase(_, Config) ->
  Config.

-spec end_per_testcase(atom(), shards_ct:config()) -> shards_ct:config().
end_per_testcase(_, Config) ->
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-spec t_join_leave_ops(any()) -> any().
t_join_leave_ops(Config) ->
  {_, Nodes} = lists:keyfind(nodes, 1, Config),
  erlang:display(Nodes),

  ResL = shards_dist_cluster:new_table(Nodes, test, [named_table, public]),
  erlang:display(ResL),

  true = shards_dist_utils:wait_until(fun() ->
    length(shards_dist_node:list(test)) =:= 5
  end),

  erlang:display({"nodes", shards_dist_node:list(test)}),

  shards_dist:new(test, [named_table, public]),

  true = shards_dist_utils:wait_until(fun() ->
    length(shards_dist_node:list(test)) =:= 6
  end),

  erlang:display({"nodes", shards_dist_node:list(test)}),

  NewNodes = shards_dist_cluster:start_slaves(['f@127.0.0.1']),

  _ = shards_dist_cluster:new_table(NewNodes, test, [named_table, public]),

  true = shards_dist_utils:wait_until(fun() ->
    length(shards_dist_node:list(test)) =:= 7
  end),

  erlang:display({"nodes", shards_dist_node:list(test)}),

  _ = shards_dist_cluster:stop_slaves(NewNodes),
  ok.
