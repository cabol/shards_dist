-module(shards_dist_node).

%% API
-export([
  list/1,
  hash_slot/2
]).

%%%===================================================================
%%% API
%%%===================================================================

list(Tab) ->
  lists:usort([node(Pid) || Pid <- shards_dist_pg:get_members(Tab)]).

hash_slot(Tab, Key) ->
  Nodes = list(Tab),
  KeyslotFun = shards:get_meta(Tab, '$keyslot_dist_fun', fun erlang:phash2/2),
  lists:nth(KeyslotFun(Key, length(Nodes)) + 1, Nodes).
