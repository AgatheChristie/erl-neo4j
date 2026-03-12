-module(neo4j_messages).

-include("neo4j.hrl").

-export([
    hello/1, hello/2, hello/3,
    logon/1, logon/2, logoff/0, goodbye/0, reset/0,
    run/1, run/2, run/3,
    discard/0, discard/1,
    pull/0, pull/1,
    begin_tx/0, begin_tx/1,
    commit/0, rollback/0,
    route/0, route/1, route/2, route/3,
    parse_response/1,
    encode_message/1, chunk_message/1,
    decode_message/1, decode_message/2,
    decode_messages/1, decode_messages/2,
    signature_name/1, summary_message/1
]).

%% ===================================================================
%% Request Messages
%% ===================================================================

hello(UserAgent) -> hello(UserAgent, #{}).
hello(UserAgent, Auth) -> hello(UserAgent, Auth, []).
hello(UserAgent, Auth, Opts) ->
    Extra0 = #{<<"user_agent">> => UserAgent},
    Extra1 = case proplists:get_value(routing, Opts) of
        undefined -> Extra0;
        Routing -> Extra0#{<<"routing">> => Routing}
    end,
    Extra2 = case proplists:get_value(bolt_agent, Opts) of
        undefined -> Extra1;
        BoltAgent -> Extra1#{<<"bolt_agent">> => BoltAgent}
    end,
    Extra = maps:merge(Extra2, Auth),
    {struct, ?BOLT_HELLO, [Extra]}.

logon(Scheme) -> logon(Scheme, #{}).
logon(Scheme, Auth) ->
    AuthMap = Auth#{<<"scheme">> => Scheme},
    {struct, ?BOLT_LOGON, [AuthMap]}.

logoff() -> {struct, ?BOLT_LOGOFF, []}.
goodbye() -> {struct, ?BOLT_GOODBYE, []}.
reset() -> {struct, ?BOLT_RESET, []}.

run(Query) -> run(Query, #{}).
run(Query, Params) -> run(Query, Params, #{}).
run(Query, Params, Metadata) ->
    {struct, ?BOLT_RUN, [Query, Params, Metadata]}.

discard() -> discard(#{}).
discard(Metadata) ->
    {struct, ?BOLT_DISCARD, [Metadata]}.

pull() -> pull(#{}).
pull(Metadata) ->
    {struct, ?BOLT_PULL, [Metadata]}.

begin_tx() -> begin_tx(#{}).
begin_tx(Metadata) ->
    {struct, ?BOLT_BEGIN, [Metadata]}.

commit() -> {struct, ?BOLT_COMMIT, []}.
rollback() -> {struct, ?BOLT_ROLLBACK, []}.

route() -> route(#{}).
route(Routing) -> route(Routing, []).
route(Routing, Bookmarks) -> route(Routing, Bookmarks, undefined).
route(Routing, Bookmarks, Db) ->
    Meta0 = #{<<"routing">> => Routing, <<"bookmarks">> => Bookmarks},
    Meta = case Db of
        undefined -> Meta0;
        _ -> Meta0#{<<"db">> => Db}
    end,
    {struct, ?BOLT_ROUTE, [Meta]}.

%% ===================================================================
%% Response Parsing
%% ===================================================================

parse_response({struct, ?BOLT_SUCCESS, [Metadata]}) ->
    {success, Metadata};
parse_response({struct, ?BOLT_FAILURE, [Metadata]}) ->
    {failure, Metadata};
parse_response({struct, ?BOLT_IGNORED, []}) ->
    {ignored, #{}};
parse_response({struct, ?BOLT_IGNORED, [Metadata]}) ->
    {ignored, Metadata};
parse_response({struct, ?BOLT_RECORD, [Values]}) when is_list(Values) ->
    {record, Values};
parse_response({struct, Signature, Fields}) ->
    {unknown, Signature, Fields};
parse_response(Other) ->
    {error, {invalid_response, Other}}.

%% ===================================================================
%% Message Encoding with Chunking
%% ===================================================================

encode_message(Message) ->
    Encoded = neo4j_packstream:encode(Message),
    chunk_message(Encoded).

chunk_message(Data) ->
    chunk_message(Data, []).

chunk_message(<<>>, Acc) ->
    iolist_to_binary(lists:reverse([<<0:16>> | Acc]));
chunk_message(Data, Acc) when byte_size(Data) =< 65535 ->
    Size = byte_size(Data),
    Chunk = <<Size:16, Data/binary>>,
    chunk_message(<<>>, [Chunk | Acc]);
chunk_message(Data, Acc) ->
    <<Chunk:65535/binary, Rest/binary>> = Data,
    chunk_message(Rest, [<<65535:16, Chunk/binary>> | Acc]).

%% ===================================================================
%% Message Decoding with De-chunking
%% ===================================================================

decode_message(Data) ->
    decode_message(Data, <<>>).

decode_message(<<0:16, Rest/binary>>, Buffer) when Buffer =/= <<>> ->
    case neo4j_packstream:decode(Buffer) of
        {ok, Message, <<>>} ->
            {ok, Message, Rest};
        {ok, _Message, _Leftover} ->
            {error, invalid_message_format};
        {error, Reason} ->
            {error, Reason}
    end;
decode_message(<<Size:16, Rest/binary>>, Buffer) when Size > 0 ->
    case byte_size(Rest) >= Size of
        true ->
            <<Chunk:Size/binary, Remaining/binary>> = Rest,
            decode_message(Remaining, <<Buffer/binary, Chunk/binary>>);
        false ->
            {incomplete}
    end;
decode_message(<<0:16, _Rest/binary>>, <<>>) ->
    {error, empty_message};
decode_message(Data, _Buffer) when byte_size(Data) < 2 ->
    {incomplete};
decode_message(_, _) ->
    {incomplete}.

decode_messages(Data) ->
    decode_messages(Data, []).

decode_messages(<<>>, Acc) ->
    {ok, lists:reverse(Acc), <<>>};
decode_messages(Data, Acc) ->
    case decode_message(Data) of
        {ok, Message, Rest} ->
            decode_messages(Rest, [Message | Acc]);
        {incomplete} ->
            {ok, lists:reverse(Acc), Data};
        {error, Reason} ->
            {error, Reason}
    end.

%% ===================================================================
%% Utility
%% ===================================================================

signature_name(?BOLT_HELLO) -> "HELLO";
signature_name(?BOLT_LOGON) -> "LOGON";
signature_name(?BOLT_LOGOFF) -> "LOGOFF";
signature_name(?BOLT_GOODBYE) -> "GOODBYE";
signature_name(?BOLT_RESET) -> "RESET";
signature_name(?BOLT_RUN) -> "RUN";
signature_name(?BOLT_DISCARD) -> "DISCARD";
signature_name(?BOLT_PULL) -> "PULL";
signature_name(?BOLT_BEGIN) -> "BEGIN";
signature_name(?BOLT_COMMIT) -> "COMMIT";
signature_name(?BOLT_ROLLBACK) -> "ROLLBACK";
signature_name(?BOLT_ROUTE) -> "ROUTE";
signature_name(?BOLT_SUCCESS) -> "SUCCESS";
signature_name(?BOLT_FAILURE) -> "FAILURE";
signature_name(?BOLT_IGNORED) -> "IGNORED";
signature_name(?BOLT_RECORD) -> "RECORD";
signature_name(Sig) -> "UNKNOWN(0x" ++ integer_to_list(Sig, 16) ++ ")".

summary_message({struct, Sig, _}) when Sig =:= ?BOLT_SUCCESS; Sig =:= ?BOLT_FAILURE -> true;
summary_message(_) -> false.
