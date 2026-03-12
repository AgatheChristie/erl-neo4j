-module(neo4j_registry).

-export([
    lookup/1, lookup_bang/1,
    registered/1, list_drivers/0
]).

%% ===================================================================
%% Public API
%% ===================================================================

lookup(DriverRef) when is_pid(DriverRef) ->
    case is_process_alive(DriverRef) of
        true -> {ok, DriverRef};
        false -> {error, not_running}
    end;
lookup(DriverName) when is_atom(DriverName) ->
    case find_driver_in_tree(DriverName) of
        {ok, Pid} -> {ok, Pid};
        not_found -> {error, not_found}
    end;
lookup(DriverRef) ->
    {error, {invalid_driver_ref, DriverRef}}.

lookup_bang(DriverRef) ->
    case lookup(DriverRef) of
        {ok, Driver} -> Driver;
        {error, not_found} -> error({neo4j_driver_not_found, DriverRef});
        {error, not_running} -> error({neo4j_driver_not_running, DriverRef});
        {error, {invalid_driver_ref, Ref}} -> error({invalid_driver_ref, Ref})
    end.

registered(DriverName) when is_atom(DriverName) ->
    case lookup(DriverName) of
        {ok, _} -> true;
        {error, _} -> false
    end.

list_drivers() ->
    case find_supervisor() of
        {ok, Supervisor} ->
            Children = supervisor:which_children(Supervisor),
            lists:filtermap(
                fun({Id, Pid, _Type, Modules}) ->
                    case is_pid(Pid) andalso is_process_alive(Pid) andalso
                         lists:member(neo4j_driver, Modules) andalso is_atom(Id) of
                        true -> {true, Id};
                        false -> false
                    end
                end,
                Children
            );
        not_found ->
            []
    end.

%% ===================================================================
%% Internal
%% ===================================================================

find_driver_in_tree(DriverName) ->
    case find_supervisor() of
        {ok, Supervisor} ->
            find_child_by_id(Supervisor, DriverName);
        not_found ->
            not_found
    end.

find_supervisor() ->
    case whereis(neo4j_sup) of
        Pid when is_pid(Pid) -> {ok, Pid};
        undefined -> not_found
    end.

find_child_by_id(Supervisor, ChildId) ->
    Children = supervisor:which_children(Supervisor),
    case lists:keyfind(ChildId, 1, Children) of
        {ChildId, Pid, _Type, _Modules} when is_pid(Pid) ->
            case is_process_alive(Pid) of
                true -> {ok, Pid};
                false -> not_found
            end;
        _ ->
            not_found
    end.
