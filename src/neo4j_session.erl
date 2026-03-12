-module(neo4j_session).

-include("neo4j.hrl").

-export([
    run/2, run/3, run/4,
    begin_transaction/1, begin_transaction/2,
    close/1, info/1
]).

%% ===================================================================
%% Public API
%% ===================================================================

run(Session, Query) ->
    run(Session, Query, #{}, []).

run(Session, Query, Params) ->
    run(Session, Query, Params, []).

run(Session, Query, Params, Opts) ->
    #{socket := Socket, config := Config} = Session,
    Timeout = proplists:get_value(timeout, Opts, maps:get(query_timeout, Config)),

    erlang:erase({message_buffer, Socket}),

    RunMsg = neo4j_messages:run(Query, Params, #{}),
    EncodedRun = neo4j_messages:encode_message(RunMsg),

    case neo4j_socket:send(Socket, EncodedRun) of
        ok ->
            case receive_message(Socket, Timeout) of
                {ok, RunResponse} ->
                    case neo4j_messages:parse_response(RunResponse) of
                        {success, Metadata} ->
                            Fields = maps:get(<<"fields">>, Metadata, []),
                            PullMsg = neo4j_messages:pull(#{<<"n">> => -1}),
                            EncodedPull = neo4j_messages:encode_message(PullMsg),
                            case neo4j_socket:send(Socket, EncodedPull) of
                                ok ->
                                    Result = collect_results(Socket, Timeout, Fields, []),
                                    erlang:erase({message_buffer, Socket}),
                                    Result;
                                {error, Reason} ->
                                    erlang:erase({message_buffer, Socket}),
                                    {error, Reason}
                            end;
                        {failure, Meta} ->
                            erlang:erase({message_buffer, Socket}),
                            {error, {query_failed, maps:get(<<"message">>, Meta, <<>>)}};
                        Other ->
                            erlang:erase({message_buffer, Socket}),
                            {error, {unexpected_response, Other}}
                    end;
                {error, Reason} ->
                    erlang:erase({message_buffer, Socket}),
                    {error, Reason}
            end;
        {error, Reason} ->
            erlang:erase({message_buffer, Socket}),
            {error, Reason}
    end.

begin_transaction(Session) ->
    begin_transaction(Session, []).

begin_transaction(Session, Opts) ->
    #{socket := Socket, config := Config} = Session,
    Timeout = proplists:get_value(timeout, Opts, maps:get(query_timeout, Config)),
    Metadata = build_transaction_metadata(Opts),

    erlang:erase({message_buffer, Socket}),

    BeginMsg = neo4j_messages:begin_tx(Metadata),
    EncodedBegin = neo4j_messages:encode_message(BeginMsg),
    case neo4j_socket:send(Socket, EncodedBegin) of
        ok ->
            case receive_message(Socket, Timeout) of
                {ok, Response} ->
                    case neo4j_messages:parse_response(Response) of
                        {success, _Meta} ->
                            Tx = #{
                                session => Session,
                                socket => Socket,
                                config => Config,
                                metadata => Metadata
                            },
                            {ok, Tx};
                        {failure, Meta} ->
                            {error, {transaction_failed, maps:get(<<"message">>, Meta, <<>>)}};
                        Other ->
                            {error, {unexpected_response, Other}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

close(Session) ->
    #{socket := Socket} = Session,
    erlang:erase({message_buffer, Socket}),
    GoodbyeMsg = neo4j_messages:goodbye(),
    neo4j_socket:send(Socket, neo4j_messages:encode_message(GoodbyeMsg)),
    neo4j_socket:close(Socket),
    ok.

info(Session) ->
    #{
        config => maps:get(config, Session),
        transaction => maps:get(transaction, Session, undefined)
    }.

%% ===================================================================
%% Internal
%% ===================================================================

collect_results(Socket, Timeout, Fields, Acc) ->
    case receive_message(Socket, Timeout) of
        {ok, Response} ->
            case neo4j_messages:parse_response(Response) of
                {record, Values} ->
                    Record = neo4j_record:new(Values, Fields),
                    collect_results(Socket, Timeout, Fields, [Record | Acc]);
                {success, Metadata} ->
                    Summary = neo4j_summary:new(Metadata),
                    Results = #{
                        records => lists:reverse(Acc),
                        summary => Summary
                    },
                    {ok, Results};
                {failure, Meta} ->
                    {error, {query_execution_failed, maps:get(<<"message">>, Meta, <<>>)}};
                Other ->
                    {error, {unexpected_response, Other}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

receive_message(Socket, Timeout) ->
    receive_message(Socket, Timeout, <<>>).

receive_message(Socket, Timeout, Buffer) ->
    BufferedData = case erlang:get({message_buffer, Socket}) of
        undefined -> <<>>;
        Buf -> Buf
    end,
    CombinedBuffer = <<BufferedData/binary, Buffer/binary>>,
    case neo4j_messages:decode_message(CombinedBuffer) of
        {ok, Message, Remaining} ->
            case byte_size(Remaining) > 0 of
                true -> erlang:put({message_buffer, Socket}, Remaining);
                false -> erlang:erase({message_buffer, Socket})
            end,
            {ok, Message};
        {incomplete} ->
            case neo4j_socket:recv(Socket, [{timeout, Timeout}]) of
                {ok, RecvData} ->
                    FullData = <<CombinedBuffer/binary, RecvData/binary>>,
                    case neo4j_messages:decode_message(FullData) of
                        {ok, Message, Remaining} ->
                            case byte_size(Remaining) > 0 of
                                true -> erlang:put({message_buffer, Socket}, Remaining);
                                false -> erlang:erase({message_buffer, Socket})
                            end,
                            {ok, Message};
                        {incomplete} ->
                            erlang:put({message_buffer, Socket}, FullData),
                            receive_message(Socket, Timeout, <<>>);
                        {error, Reason} ->
                            erlang:erase({message_buffer, Socket}),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    erlang:erase({message_buffer, Socket}),
                    {error, Reason}
            end;
        {error, Reason} ->
            erlang:erase({message_buffer, Socket}),
            {error, Reason}
    end.

build_transaction_metadata(Opts) ->
    M0 = #{},
    M1 = case proplists:get_value(mode, Opts) of
        undefined -> M0;
        Mode -> M0#{<<"mode">> => Mode}
    end,
    case proplists:get_value(timeout, Opts) of
        undefined -> M1;
        TxTimeout -> M1#{<<"tx_timeout">> => TxTimeout}
    end.
