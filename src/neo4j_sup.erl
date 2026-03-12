-module(neo4j_sup).
-behaviour(supervisor).

-export([start_link/2]).
-export([init/1]).

start_link(SupFlags, Children) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {SupFlags, Children}).

init({SupFlags, Children}) ->
    {ok, {SupFlags, Children}}.
