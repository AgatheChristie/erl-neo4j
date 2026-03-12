-module(neo4j_socket).

-export([connect/3, send/2, recv/1, recv/2, close/1, setopts/2, getopts/2]).

-define(DEFAULT_TIMEOUT, 15000).
-define(TCP_OPTS, [binary, {packet, raw}, {active, false}, {nodelay, true}, inet]).

connect(Host, Port, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    AdditionalOpts = proplists:get_value(tcp_opts, Opts, []),
    TcpOpts = ?TCP_OPTS ++ AdditionalOpts,
    HostCharlist = host_to_charlist(Host),
    case gen_tcp:connect(HostCharlist, Port, TcpOpts, Timeout) of
        {ok, Socket} ->
            {ok, Socket};
        {error, Reason} ->
            {error, {connection_failed, Reason}}
    end.

send(Socket, Data) when is_binary(Data) ->
    case gen_tcp:send(Socket, Data) of
        ok -> ok;
        {error, Reason} -> {error, {send_failed, Reason}}
    end.

recv(Socket) ->
    recv(Socket, []).

recv(Socket, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, ?DEFAULT_TIMEOUT),
    Length = proplists:get_value(length, Opts, 0),
    case gen_tcp:recv(Socket, Length, Timeout) of
        {ok, Data} ->
            {ok, Data};
        {error, Reason} ->
            {error, {recv_failed, Reason}}
    end.

close(Socket) ->
    gen_tcp:close(Socket).

setopts(Socket, Opts) ->
    inet:setopts(Socket, Opts).

getopts(Socket, Opts) ->
    inet:getopts(Socket, Opts).

%% Internal

host_to_charlist(Host) when is_binary(Host) -> binary_to_list(Host);
host_to_charlist(Host) when is_list(Host) -> Host;
host_to_charlist(Host) when is_atom(Host) -> atom_to_list(Host).
