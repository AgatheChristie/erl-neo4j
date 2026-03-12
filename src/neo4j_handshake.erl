-module(neo4j_handshake).

-include("neo4j.hrl").

-export([
    perform/1, send_handshake/1, receive_version/1,
    build_handshake_data/0, encode_version/1, parse_version/1,
    supported_version/1, supported_versions/0
]).

-define(BOLT_VERSIONS, [{5, 4}, {5, 3}, {4, 4}, {4, 3}]).

perform(Socket) ->
    case send_handshake(Socket) of
        ok ->
            receive_version(Socket);
        {error, _} = Err ->
            Err
    end.

send_handshake(Socket) ->
    Data = build_handshake_data(),
    neo4j_socket:send(Socket, Data).

receive_version(Socket) ->
    case neo4j_socket:recv(Socket, [{length, 4}]) of
        {ok, <<0, 0, 0, 0>>} ->
            logger:error("Handshake: version negotiation failed"),
            {error, version_negotiation_failed};
        {ok, VersionBytes} ->
            parse_version(VersionBytes);
        {error, Reason} ->
            logger:error("Handshake: socket recv failed - ~p", [Reason]),
            {error, Reason}
    end.

build_handshake_data() ->
    VersionBytes = [encode_version(V) || V <- ?BOLT_VERSIONS],
    PaddingCount = 4 - length(VersionBytes),
    Padding = [<<0, 0, 0, 0>> || _ <- lists:seq(1, PaddingCount)],
    iolist_to_binary([?BOLT_MAGIC | VersionBytes ++ Padding]).

encode_version({Major, Minor}) ->
    <<Minor:8, 0:8, 0:8, Major:8>>.

parse_version(<<Minor:8, 0:8, 0:8, Major:8>>) ->
    {ok, {Major, Minor}};
parse_version(<<0:8, 0:8, Minor:8, Major:8>>) ->
    {ok, {Major, Minor}};
parse_version(<<Major:8, Minor:8, 0:8, 0:8>>) ->
    {ok, {Major, Minor}};
parse_version(<<0:8, Major:8, 0:8, Minor:8>>) ->
    {ok, {Major, Minor}};
parse_version(Bytes) ->
    logger:error("Handshake: unrecognized version format - ~p", [Bytes]),
    {error, invalid_version_format}.

supported_version({Major, Minor}) ->
    lists:member({Major, Minor}, ?BOLT_VERSIONS).

supported_versions() ->
    ?BOLT_VERSIONS.
