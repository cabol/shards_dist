%%%-------------------------------------------------------------------
%%% @doc
%%% General purpose utilities..
%%% @end
%%%-------------------------------------------------------------------
-module(shards_dist_utils).

%% API
-export([
  wait_until/1,
  wait_until/2,
  wait_until/3
]).

%%%===================================================================
%%% API
%%%===================================================================

wait_until(Fun) ->
  wait_until(Fun, 50).

wait_until(Fun, Retries) ->
  wait_until(Fun, Retries, 100).

wait_until(Fun, 0, _Timeout) ->
  Fun();
wait_until(Fun, Retries, Timeout) ->
  case Fun() of
    true ->
      true;

    false ->
      ok = timer:sleep(Timeout),
      wait_until(Fun, decr_retries(Retries), Timeout)
  end.

decr_retries(infinity) -> infinity;
decr_retries(Val) when is_integer(Val), Val > 0 -> Val - 1.
