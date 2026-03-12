-module(neo4j_driver).
-behaviour(gen_server).

-export([
    start_link/1, start_link/2,
    run/2, run/3, run/4,
    session/2, transaction/2,
    create_session/1, close_session/1,
    close/1, get_config/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(DEFAULT_CONFIG, #{
    host => "localhost",
    port => 7687,
    auth => undefined,
    user_agent => "neo4j_ex/0.1.0",
    max_pool_size => 10,
    connection_timeout => 15000,
    query_timeout => 30000
}).

%% ===================================================================
%% Client API
%% ===================================================================

start_link(Uri) ->
    start_link(Uri, []).

start_link(Uri, Opts) ->
    Config = parse_uri_and_opts(Uri, Opts),
    case proplists:get_value(name, Opts) of
        undefined ->
            gen_server:start_link(?MODULE, Config, []);
        Name ->
            gen_server:start_link({local, Name}, ?MODULE, Config, [])
    end.

run(Driver, Query) ->
    run(Driver, Query, #{}, []).
run(Driver, Query, Params) ->
    run(Driver, Query, Params, []).
run(Driver, Query, Params, Opts) ->
    session(Driver, fun(Session) ->
        neo4j_session:run(Session, Query, Params, Opts)
    end).

session(Driver, Fun) when is_function(Fun, 1) ->
    case create_session(Driver) of
        {ok, Session} ->
            try
                Fun(Session)
            after
                close_session(Session)
            end;
        {error, _} = Err ->
            Err
    end.

transaction(Driver, Fun) when is_function(Fun, 1) ->
    session(Driver, fun(Session) ->
        neo4j_transaction:execute(Session, Fun)
    end).

create_session(Driver) ->
    gen_server:call(Driver, create_session).

close_session(Session) ->
    neo4j_session:close(Session).

close(Driver) ->
    gen_server:call(Driver, close).

get_config(Driver) ->
    gen_server:call(Driver, get_config).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init(Config) ->
    State = #{
        config => Config,
        connections => [],
        sessions => []
    },
    {ok, State}.

handle_call(create_session, _From, #{config := Config, sessions := Sessions} = State) ->
    case create_connection(Config) of
        {ok, Socket} ->
            Session = #{
                socket => Socket,
                config => Config,
                transaction => undefined
            },
            NewState = State#{sessions => [Session | Sessions]},
            {reply, {ok, Session}, NewState};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call(close, _From, #{connections := Conns, sessions := Sessions} = State) ->
    [neo4j_socket:close(C) || C <- Conns],
    [neo4j_socket:close(maps:get(socket, S)) || S <- Sessions],
    {reply, ok, State#{connections => [], sessions => []}};

handle_call(get_config, _From, #{config := Config} = State) ->
    {reply, Config, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{connections := Conns, sessions := Sessions}) ->
    [neo4j_socket:close(C) || C <- Conns],
    [neo4j_socket:close(maps:get(socket, S)) || S <- Sessions],
    ok.

%% ===================================================================
%% Internal
%% ===================================================================

parse_uri_and_opts(Uri, Opts) ->
    UriConfig = parse_uri(Uri),
    OptsMap = maps:from_list([{K, V} || {K, V} <- Opts, K =/= name]),
    Config0 = maps:merge(?DEFAULT_CONFIG, UriConfig),
    Config1 = maps:merge(Config0, OptsMap),
    Config1#{auth => normalize_auth(maps:get(auth, Config1))}.

parse_uri("bolt://" ++ Rest) ->
    case string:split(Rest, ":", trailing) of
        [Host, Port] ->
            #{host => Host, port => list_to_integer(Port)};
        [Host] ->
            #{host => Host}
    end;
parse_uri(<<"bolt://", Rest/binary>>) ->
    parse_uri("bolt://" ++ binary_to_list(Rest));
parse_uri(Uri) ->
    error({unsupported_uri_scheme, Uri}).

normalize_auth(undefined) -> #{};
normalize_auth({Username, Password}) ->
    #{
        <<"scheme">> => <<"basic">>,
        <<"principal">> => to_bin(Username),
        <<"credentials">> => to_bin(Password)
    };
normalize_auth(Auth) when is_map(Auth) -> Auth.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_list(V) -> list_to_binary(V);
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8).

create_connection(Config) ->
    Host = maps:get(host, Config),
    Port = maps:get(port, Config),
    Timeout = maps:get(connection_timeout, Config),
    case neo4j_socket:connect(Host, Port, [{timeout, Timeout}]) of
        {ok, Socket} ->
            case neo4j_handshake:perform(Socket) of
                {ok, _Version} ->
                    case authenticate(Socket, Config) of
                        ok ->
                            {ok, Socket};
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
    UserAgent = to_bin(maps:get(user_agent, Config)),
    Auth = maps:get(auth, Config),
    BoltAgent = #{
        <<"product">> => UserAgent,
        <<"language">> => <<"Erlang">>,
        <<"language_version">> => list_to_binary(erlang:system_info(otp_release))
    },
    HelloMsg = neo4j_messages:hello(UserAgent, Auth, [{bolt_agent, BoltAgent}]),
    EncodedHello = neo4j_messages:encode_message(HelloMsg),
    case neo4j_socket:send(Socket, EncodedHello) of
        ok ->
            case receive_message(Socket, 15000) of
                {ok, Response} ->
                    case neo4j_messages:parse_response(Response) of
                        {success, _Metadata} ->
                            ok;
                        {failure, Metadata} ->
                            Msg = maps:get(<<"message">>, Metadata, <<"auth failed">>),
                            {error, {auth_failed, Msg}};
                        Other ->
                            {error, {unexpected_response, Other}}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

receive_message(Socket, Timeout) ->
    receive_message(Socket, Timeout, <<>>).

receive_message(Socket, Timeout, Buffer) ->
    case neo4j_socket:recv(Socket, [{timeout, Timeout}]) of
        {ok, Data} ->
            FullData = <<Buffer/binary, Data/binary>>,
            case neo4j_messages:decode_message(FullData) of
                {ok, Message, _Rest} ->
                    {ok, Message};
                {incomplete} ->
                    receive_message(Socket, Timeout, FullData);
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
