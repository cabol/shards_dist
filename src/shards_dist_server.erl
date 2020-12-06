%%%-------------------------------------------------------------------
%%% @doc
%%% Server.
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist_server).

-behaviour(gen_server).

%% API
-export([
  start_link/2,
  stop/1,
  cluster_sync_resource_id/0
]).

%% gen_server callbacks
-export([
  init/1,
  handle_continue/2,
  handle_call/3,
  handle_cast/2,
  handle_info/2
]).

%% Inline-compiled functions
-compile({inline, [cluster_sync_resource_id/0]}).

%% Internal state
-record(state, {
  tab       :: atom(),
  opts      :: [term()],
  sync_hist :: [{integer(), non_neg_integer()}]
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Tab, Opts) when is_atom(Tab) ->
  Name = binary_to_atom(<<(atom_to_binary(Tab, utf8))/binary, "_server">>, utf8),
  gen_server:start_link({local, Name}, ?MODULE, {Tab, Opts}, []).

stop(Pid) ->
  gen_server:call(Pid, stop).

cluster_sync_resource_id() -> {?MODULE, cluster_sync}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
init({Tab, Opts}) ->
  NewOpts = lists:usort([named_table | Opts]),
  Tab = shards:new(Tab, NewOpts),
  {ok, #state{tab = Tab, opts = NewOpts}, {continue, join}}.

%% @hidden
handle_continue(join, #state{tab = Tab} = State) ->
  ok = do_join(Tab),
  ok = broadcast(Tab, {node_attached, node()}),
  {noreply, State}.

%% @hidden
handle_call(stop, _From, #state{tab = Tab} = State) ->
  ok = shards_dist_pg:leave(Tab),
  true = shards:delete(Tab),
  {reply, ok, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%% @hidden
handle_cast(_Request, State) ->
  {noreply, State}.

%% @hidden
handle_info({node_attached, Node}, #state{tab = Tab} = State) ->
  erlang:display(["NEW NODE", Node, shards_dist_node:list(Tab)]),
  true = ensure_node_attached(Tab, Node),
  {noreply, cluster_sync(State)};
handle_info({node_detached, Node}, #state{tab = Tab} = State) ->
  true = ensure_node_detached(Tab, Node),
  {noreply, cluster_sync(State)}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
ensure_node_attached(Tab, Node) ->
  % @TODO: Maybe fail after X period of time
  shards_dist_utils:wait_until(fun() ->
    lists:member(Node, shards_dist_node:list(Tab))
  end, infinity).

%% @private
ensure_node_detached(Tab, Node) ->
  % @TODO: Maybe fail after X period of time
  shards_dist_utils:wait_until(fun() ->
    not lists:member(Node, shards_dist_node:list(Tab))
  end, infinity).

%% @private
cluster_sync(#state{tab = Tab, sync_hist = Sync} = State) ->
  % Run rehashing job
  global:trans({cluster_sync_resource_id(), self()}, fun() ->
    true = shards:safe_fixtable(Tab, true),

    RehashedKeys =
      shards:foldl(fun(Object, Acc) ->
        ok = shards_dist:rehash(Tab, Object),
        Acc + 1
      end, 0, Tab),

    true = shards:safe_fixtable(Tab, false),
    State#state{sync_hist = [{erlang:system_time(), RehashedKeys} | Sync]}
  end, [node()]).

%% @private
do_join(Tab) ->
  case lists:member(self(), shards_dist_pg:get_members(Tab)) of
    true  -> ok;
    false -> shards_dist_pg:join(Tab, self())
  end.

%% @private
broadcast(Tab, Message) ->
  lists:foreach(fun(Pid) ->
    Pid ! Message
  end, shards_dist_pg:get_members(Tab) -- [self()]).
