-module(neo4j_pool_worker).
-behaviour(gen_server).

-export([start_link/1, run/2, run/3, run/4, transaction/2, transaction/3, status/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    socket :: port() | undefined,
    config :: map(),
    connected = false :: boolean()
}).

%% ===================================================================
%% Client API
%% ===================================================================

start_link(ConnConfig) ->
    gen_server:start_link(?MODULE, ConnConfig, []).

run(Worker, Query) -> run(Worker, Query, #{}, []).
run(Worker, Query, Params) -> run(Worker, Query, Params, []).
run(Worker, Query, Params, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, 30000),
    gen_server:call(Worker, {run, Query, Params, Opts}, Timeout).

transaction(Worker, Fun) -> transaction(Worker, Fun, []).
transaction(Worker, Fun, Opts) when is_function(Fun, 0) ->
    Timeout = proplists:get_value(timeout, Opts, 30000),
    gen_server:call(Worker, {transaction, Fun, Opts}, Timeout).

status(Worker) ->
    gen_server:call(Worker, status).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init(ConnConfig) ->
    ConfigMap = maps:from_list(ConnConfig),
    State = #state{socket = undefined, config = ConfigMap, connected = false},
    case connect(State) of
        {ok, NewState} ->
            {ok, NewState};
        {error, _Reason} ->
            {ok, State}
    end.

handle_call({run, Query, Params, Opts}, _From, State) ->
    case ensure_connected(State) of
        {ok, ConnState} ->
            case execute_query(ConnState, Query, Params, Opts) of
                {ok, Result} ->
                    {reply, {ok, Result}, ConnState};
                {error, Reason} ->
                    {reply, {error, Reason}, ConnState#state{connected = false}}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({transaction, Fun, _Opts}, _From, State) ->
    case ensure_connected(State) of
        {ok, ConnState} ->
            case execute_transaction(ConnState, Fun) of
                {ok, Result} ->
                    {reply, {ok, Result}, ConnState};
                {error, Reason} ->
                    {reply, {error, Reason}, ConnState#state{connected = false}}
            end;
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(status, _From, #state{connected = Connected} = State) ->
    Status = case Connected of true -> connected; false -> disconnected end,
    {reply, Status, State}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, #state{socket = Socket}) ->
    case Socket of
        undefined -> ok;
        _ -> neo4j_socket:close(Socket)
    end,
    ok.

%% ===================================================================
%% Internal
%% ===================================================================

ensure_connected(#state{connected = true} = State) -> {ok, State};
ensure_connected(State) -> connect(State).

connect(#state{config = Config} = State) ->
    Host = maps:get(host, Config, "localhost"),
    Port = maps:get(port, Config, 7687),
    Timeout = maps:get(connection_timeout, Config, 15000),
    case neo4j_socket:connect(Host, Port, [{timeout, Timeout}]) of
        {ok, Socket} ->
            case neo4j_handshake:perform(Socket) of
                {ok, _Version} ->
                    case authenticate(Socket, Config) of
                        ok ->
                            {ok, State#state{socket = Socket, connected = true}};
                        {error, Reason} ->
                            neo4j_socket:close(Socket),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    neo4j_socket:close(Socket),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(Socket, Config) ->
    UserAgent = to_bin(maps:get(user_agent, Config, "neo4j_ex/0.1.0")),
    Auth = maps:get(auth, Config, #{}),
    BoltAgent = #{
        <<"product">> => UserAgent,
        <<"language">> => <<"Erlang">>,
        <<"language_version">> => list_to_binary(erlang:system_info(otp_release))
    },
    HelloMsg = neo4j_messages:hello(UserAgent, Auth, [{bolt_agent, BoltAgent}]),
    EncodedHello = neo4j_messages:encode_message(HelloMsg),
    case neo4j_socket:send(Socket, EncodedHello) of
        ok ->
            case recv_message(Socket, 15000) of
                {ok, Response} ->
                    case neo4j_messages:parse_response(Response) of
                        {success, _Meta} -> ok;
                        {failure, Meta} ->
                            {error, {auth_failed, maps:get(<<"message">>, Meta, <<>>)}};
                        _ -> {error, unexpected_response}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

execute_query(#state{socket = Socket, config = Config}, Query, Params, Opts) ->
    Session = #{socket => Socket, config => Config, transaction => undefined},
    neo4j_session:run(Session, Query, Params, Opts).

execute_transaction(#state{socket = Socket, config = Config}, Fun) ->
    Session = #{socket => Socket, config => Config, transaction => undefined},
    WrappedFun = fun(_Tx) -> Fun() end,
    neo4j_transaction:execute(Session, WrappedFun).

recv_message(Socket, Timeout) ->
    recv_message(Socket, Timeout, <<>>).

recv_message(Socket, Timeout, Buffer) ->
    case neo4j_socket:recv(Socket, [{timeout, Timeout}]) of
        {ok, Data} ->
            FullData = <<Buffer/binary, Data/binary>>,
            case neo4j_messages:decode_message(FullData) of
                {ok, Message, _Rest} -> {ok, Message};
                {incomplete} -> recv_message(Socket, Timeout, FullData);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V);
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8).
