-module(shards_dist_cluster).

-export([
  start/1,
  stop/1,
  start_slaves/1,
  stop_slaves/1,
  new_table/3
]).

-define(PRIMARY, 'primary@127.0.0.1').

%%%===================================================================
%%% API
%%%===================================================================

start(Nodes) ->
  ok = start_primary_node(),
  ok = allow_boot(),
  Slaves = start_slaves(Nodes),
  {ok, Slaves}.

stop(Nodes) ->
  _ = stop_slaves(Nodes -- [?PRIMARY]),
  ok.

start_slaves(Slaves) ->
  start_slaves(Slaves, []).

stop_slaves(Slaves) ->
  stop_slaves(Slaves, []).

new_table(Nodes, Tab, Opts) ->
  {ResL, []} = rpc:multicall(Nodes, erlang, spawn_monitor, [shards_dist, new, [Tab, Opts]]),
  ResL.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
start_primary_node() ->
  {ok, _} = net_kernel:start([?PRIMARY]),
  true = erlang:set_cookie(node(), shards),
  ok.

%% @private
allow_boot() ->
  _ = erl_boot_server:start([]),
  {ok, IPv4} = inet:parse_ipv4_address("127.0.0.1"),
  erl_boot_server:add_slave(IPv4).

%% @private
start_slaves([], Acc) ->
  lists:usort(Acc);
start_slaves([Node | T], Acc) ->
  start_slaves(T, [spawn_node(Node) | Acc]).

%% @private
spawn_node(Node) ->
  Cookie = atom_to_list(erlang:get_cookie()),
  InetLoaderArgs = "-loader inet -hosts 127.0.0.1 -setcookie " ++ Cookie,

  {ok, Node} =
    slave:start(
      "127.0.0.1",
      node_name(Node),
      InetLoaderArgs
    ),

  ok = rpc:block_call(Node, code, add_paths, [code:get_path()]),
  {ok, _} = rpc:block_call(Node, application, ensure_all_started, [shards_dist]),
  % ok = load_support_files(Node),
  Node.

%% @private
node_name(Node) ->
  [Name, _] = binary:split(atom_to_binary(Node, utf8), <<"@">>),
  binary_to_atom(Name, utf8).

% %% @private
% load_support_files(Node) ->
%   {module, shards_tests} = rpc:block_call(Node, code, load_file, [shards_tests]),
%   ok.

%% @private
stop_slaves([], Acc) ->
  lists:usort(Acc);
stop_slaves([Node | T], Acc) ->
  ok = slave:stop(Node),
  pang = net_adm:ping(Node),
  stop_slaves(T, [Node | Acc]).
