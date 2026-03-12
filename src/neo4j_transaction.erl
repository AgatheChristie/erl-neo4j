-module(neo4j_transaction).

-include("neo4j.hrl").

-export([
    execute/2,
    run/2, run/3, run/4,
    commit/1, rollback/1,
    info/1
]).

%% ===================================================================
%% Public API
%% ===================================================================

execute(Session, Fun) when is_function(Fun, 1) ->
    case neo4j_session:begin_transaction(Session) of
        {ok, Tx} ->
            try
                Result = Fun(Tx),
                case commit(Tx) of
                    ok -> {ok, Result};
                    {error, CommitErr} -> {error, CommitErr}
                end
            catch
                throw:ThrowVal ->
                    rollback(Tx),
                    throw(ThrowVal);
                error:ErrReason:Stacktrace ->
                    rollback(Tx),
                    erlang:raise(error, ErrReason, Stacktrace);
                exit:ExitReason ->
                    rollback(Tx),
                    exit(ExitReason)
            end;
        {error, _} = Err ->
            Err
    end.

run(Tx, Query) ->
    run(Tx, Query, #{}, []).
run(Tx, Query, Params) ->
    run(Tx, Query, Params, []).
run(Tx, Query, Params, Opts) ->
    #{socket := Socket, config := Config} = Tx,
    Timeout = proplists:get_value(timeout, Opts, maps:get(query_timeout, Config)),

    RunMsg = neo4j_messages:run(Query, Params, #{}),
    EncodedRun = neo4j_messages:encode_message(RunMsg),
    case neo4j_socket:send(Socket, EncodedRun) of
        ok ->
            case receive_message(Socket, Timeout) of
                {ok, RunResponse} ->
                    case neo4j_messages:parse_response(RunResponse) of
                        {success, _Metadata} ->
                            PullMsg = neo4j_messages:pull(#{<<"n">> => -1}),
                            EncodedPull = neo4j_messages:encode_message(PullMsg),
                            case neo4j_socket:send(Socket, EncodedPull) of
                                ok ->
                                    collect_results(Socket, Timeout, []);
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        {failure, Meta} ->
                            {error, {query_failed, maps:get(<<"message">>, Meta, <<>>)}};
                        Other ->
                            {error, {unexpected_response, Other}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

commit(Tx) ->
    #{socket := Socket, config := Config} = Tx,
    Timeout = maps:get(query_timeout, Config),
    CommitMsg = neo4j_messages:commit(),
    EncodedCommit = neo4j_messages:encode_message(CommitMsg),
    case neo4j_socket:send(Socket, EncodedCommit) of
        ok ->
            case receive_message(Socket, Timeout) of
                {ok, Response} ->
                    case neo4j_messages:parse_response(Response) of
                        {success, _Meta} -> ok;
                        {failure, Meta} ->
                            {error, {commit_failed, maps:get(<<"message">>, Meta, <<>>)}};
                        Other ->
                            {error, {unexpected_response, Other}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

rollback(Tx) ->
    #{socket := Socket, config := Config} = Tx,
    Timeout = maps:get(query_timeout, Config),
    RollbackMsg = neo4j_messages:rollback(),
    EncodedRollback = neo4j_messages:encode_message(RollbackMsg),
    case neo4j_socket:send(Socket, EncodedRollback) of
        ok ->
            case receive_message(Socket, Timeout) of
                {ok, Response} ->
                    case neo4j_messages:parse_response(Response) of
                        {success, _Meta} -> ok;
                        {failure, Meta} ->
                            {error, {rollback_failed, maps:get(<<"message">>, Meta, <<>>)}};
                        Other ->
                            {error, {unexpected_response, Other}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

info(Tx) ->
    #{
        session => maps:get(session, Tx),
        metadata => maps:get(metadata, Tx)
    }.

%% ===================================================================
%% Internal
%% ===================================================================

collect_results(Socket, Timeout, Acc) ->
    case receive_message(Socket, Timeout) of
        {ok, Response} ->
            case neo4j_messages:parse_response(Response) of
                {record, Values} ->
                    Record = neo4j_record:new(Values),
                    collect_results(Socket, Timeout, [Record | Acc]);
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
