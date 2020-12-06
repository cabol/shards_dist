%%%-------------------------------------------------------------------
%%% @doc
%%% Distributed implementation for
%%% <a href="https://github.com/cabol/shards">shards</a>.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist).

%% API
-export([
  delete/1,
  delete/2,
  insert/2,
  lookup/2,
  lookup_element/3,
  new/2
]).

%% Helpers
-export([
  rehash/2
]).

%%%===================================================================
%%% Helper Macros
%%%===================================================================

%% Shared lock
-define(LOCK_ID, {shards_dist_server:cluster_sync_resource_id(), ?MODULE}).

%% Macro for wrapping the function within a global transaction
-define(with_tx(TxnFun), global:trans(?LOCK_ID, TxnFun, [node()])).

%%%===================================================================
%%% API
%%%===================================================================

delete(Tab) ->
  ?with_tx(fun() ->
    Pid = shards:get_meta(Tab, '$server_pid'),
    ok = shards_dist_server:stop(Pid),
    true
  end).

delete(Tab, Key) ->
  ?with_tx(fun() ->
    Node = shards_dist_node:hash_slot(Tab, Key),
    rpc_call(Node, shards, delete, [Tab, Key])
  end).

insert(Tab, ObjOrObjs) ->
  insert(Tab, ObjOrObjs, shards:table_meta(Tab)).

insert(Tab, ObjOrObjs, Meta) when is_list(ObjOrObjs) ->
  ?with_tx(fun() ->
    maps:fold(fun(Node, Group, Acc) ->
      Acc = rpc_call(Node, shards, insert, [Tab, Group])
    end, true, group_keys_by_node(Tab, ObjOrObjs, Meta))
  end);
insert(Tab, ObjOrObjs, Meta) when is_tuple(ObjOrObjs) ->
  ?with_tx(fun() ->
    Key = shards_lib:object_key(ObjOrObjs, Meta),
    Node = shards_dist_node:hash_slot(Tab, Key),
    true = rpc_call(Node, shards, insert, [Tab, ObjOrObjs])
  end).

lookup(Tab, Key) ->
  ?with_tx(fun() ->
    Node = shards_dist_node:hash_slot(Tab, Key),
    rpc_call(Node, shards, lookup, [Tab, Key])
  end).

lookup_element(Tab, Key, Pos) ->
  ?with_tx(fun() ->
    Node = shards_dist_node:hash_slot(Tab, Key),
    rpc_call(Node, shards, lookup_element, [Tab, Key, Pos])
  end).

new(Tab, Opts) ->
  {ok, Pid} = shards_dist_server:start_link(Tab, Opts),
  ok = shards:put_meta(Tab, '$server_pid', Pid),
  Tab.

%%%===================================================================
%%% Helpers
%%%===================================================================

rehash(Tab, Object) ->
  Node = node(),
  Meta = shards:table_meta(Tab),
  Key = shards_lib:object_key(Object, Meta),

  case shards_dist_node:hash_slot(Tab, Key) of
    Node ->
      ok;

    RemoteNode ->
      true = rpc_call(RemoteNode, shards, insert, [Tab, Object]),
      true = shards:delete(Tab, Key),
      ok
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
rpc_call(Node, Mod, Fun, Args) when Node == node() ->
  apply(Mod, Fun, Args);
rpc_call(Node, Mod, Fun, Args) ->
  case rpc:call(Node, Mod, Fun, Args) of
    {badrpc, Reason} -> error(Reason);
    Response         -> Response
  end.

%% @private
group_keys_by_node(Tab, Objects, Meta) ->
  lists:foldr(fun(Object, Acc) ->
    Key = shards_lib:object_key(Object, Meta),
    Node = shards_dist_node:hash_slot(Tab, Key),
    Acc#{Node => [Object | maps:get(Node, Acc, [])]}
  end, #{}, Objects).
