-ifndef(NEO4J_HRL).
-define(NEO4J_HRL, true).

%% ===================================================================
%% Graph Types
%% ===================================================================

-record(neo4j_node, {
    id :: integer(),
    labels = [] :: [binary()],
    properties = #{} :: map(),
    element_id :: binary() | undefined
}).

-record(neo4j_relationship, {
    id :: integer(),
    start_id :: integer(),
    end_id :: integer(),
    type :: binary(),
    properties = #{} :: map(),
    element_id :: binary() | undefined
}).

-record(neo4j_path, {
    nodes = [] :: [#neo4j_node{}],
    relationships = [] :: [#neo4j_relationship{}],
    indices = [] :: [integer()]
}).

%% ===================================================================
%% Spatial Types
%% ===================================================================

-record(neo4j_point2d, {
    x :: float(),
    y :: float(),
    srid = 4326 :: integer()
}).

-record(neo4j_point3d, {
    x :: float(),
    y :: float(),
    z :: float(),
    srid = 4979 :: integer()
}).

%% ===================================================================
%% Temporal Types
%% ===================================================================

-record(neo4j_date, {
    year :: integer(),
    month :: integer(),
    day :: integer()
}).

-record(neo4j_time, {
    hour :: integer(),
    minute :: integer(),
    second :: integer(),
    nanosecond :: integer(),
    timezone_offset_seconds :: integer()
}).

-record(neo4j_local_time, {
    hour :: integer(),
    minute :: integer(),
    second :: integer(),
    nanosecond :: integer()
}).

-record(neo4j_datetime, {
    year :: integer(),
    month :: integer(),
    day :: integer(),
    hour :: integer(),
    minute :: integer(),
    second :: integer(),
    nanosecond :: integer(),
    timezone_id :: binary() | undefined
}).

-record(neo4j_local_datetime, {
    year :: integer(),
    month :: integer(),
    day :: integer(),
    hour :: integer(),
    minute :: integer(),
    second :: integer(),
    nanosecond :: integer()
}).

-record(neo4j_duration, {
    months :: integer(),
    days :: integer(),
    seconds :: integer(),
    nanoseconds :: integer()
}).

%% ===================================================================
%% Result Types
%% ===================================================================

-record(neo4j_record, {
    values = [] :: list(),
    fields :: [binary()] | undefined
}).

-record(neo4j_summary, {
    query_type :: binary() | undefined,
    counters :: map() | undefined,
    plan :: map() | undefined,
    profile :: map() | undefined,
    notifications :: list() | undefined,
    result_available_after :: integer() | undefined,
    result_consumed_after :: integer() | undefined,
    server :: map() | undefined,
    database :: binary() | undefined
}).

%% ===================================================================
%% Session / Transaction State
%% ===================================================================

-record(neo4j_session, {
    socket :: port() | undefined,
    config :: map(),
    transaction :: undefined | term()
}).

-record(neo4j_tx, {
    session :: #neo4j_session{},
    socket :: port() | undefined,
    config :: map(),
    metadata :: map()
}).

%% ===================================================================
%% Driver Configuration
%% ===================================================================

-record(neo4j_config, {
    host = "localhost" :: string(),
    port = 7687 :: integer(),
    auth :: {string(), string()} | map() | undefined,
    user_agent = "neo4j_ex/0.1.0" :: string(),
    max_pool_size = 10 :: integer(),
    connection_timeout = 15000 :: integer(),
    query_timeout = 30000 :: integer()
}).

%% ===================================================================
%% PackStream Markers
%% ===================================================================

-define(PACKSTREAM_TINY_STRING, 16#80).
-define(PACKSTREAM_TINY_LIST, 16#90).
-define(PACKSTREAM_TINY_MAP, 16#A0).
-define(PACKSTREAM_TINY_STRUCT, 16#B0).

-define(PACKSTREAM_NULL, 16#C0).
-define(PACKSTREAM_FALSE, 16#C2).
-define(PACKSTREAM_TRUE, 16#C3).
-define(PACKSTREAM_FLOAT64, 16#C1).

-define(PACKSTREAM_INT8, 16#C8).
-define(PACKSTREAM_INT16, 16#C9).
-define(PACKSTREAM_INT32, 16#CA).
-define(PACKSTREAM_INT64, 16#CB).

-define(PACKSTREAM_STRING8, 16#D0).
-define(PACKSTREAM_STRING16, 16#D1).
-define(PACKSTREAM_STRING32, 16#D2).

-define(PACKSTREAM_LIST8, 16#D4).
-define(PACKSTREAM_LIST16, 16#D5).
-define(PACKSTREAM_LIST32, 16#D6).

-define(PACKSTREAM_MAP8, 16#D8).
-define(PACKSTREAM_MAP16, 16#D9).
-define(PACKSTREAM_MAP32, 16#DA).

-define(PACKSTREAM_STRUCT8, 16#DC).
-define(PACKSTREAM_STRUCT16, 16#DD).

%% ===================================================================
%% Bolt Message Signatures
%% ===================================================================

-define(BOLT_HELLO, 16#01).
-define(BOLT_LOGON, 16#6A).
-define(BOLT_LOGOFF, 16#6B).
-define(BOLT_GOODBYE, 16#02).
-define(BOLT_RESET, 16#0F).
-define(BOLT_RUN, 16#10).
-define(BOLT_DISCARD, 16#2F).
-define(BOLT_PULL, 16#3F).
-define(BOLT_BEGIN, 16#11).
-define(BOLT_COMMIT, 16#12).
-define(BOLT_ROLLBACK, 16#13).
-define(BOLT_ROUTE, 16#66).

-define(BOLT_SUCCESS, 16#70).
-define(BOLT_FAILURE, 16#7F).
-define(BOLT_IGNORED, 16#7E).
-define(BOLT_RECORD, 16#71).

%% ===================================================================
%% Neo4j Type Signatures (PackStream struct signatures)
%% ===================================================================

-define(NEO4J_NODE_SIG, 16#4E).
-define(NEO4J_RELATIONSHIP_SIG, 16#52).
-define(NEO4J_PATH_SIG, 16#50).
-define(NEO4J_UNBOUND_REL_SIG, 16#72).
-define(NEO4J_POINT2D_SIG, 16#58).
-define(NEO4J_POINT3D_SIG, 16#59).
-define(NEO4J_DATE_SIG, 16#44).
-define(NEO4J_TIME_SIG, 16#54).
-define(NEO4J_LOCAL_TIME_SIG, 16#74).
-define(NEO4J_DATETIME_SIG, 16#46).
-define(NEO4J_DATETIME_ZONE_ID_SIG, 16#69).
-define(NEO4J_DATETIME_LEGACY_SIG, 16#49).
-define(NEO4J_LOCAL_DATETIME_SIG, 16#64).
-define(NEO4J_DURATION_SIG, 16#45).

%% ===================================================================
%% Bolt Handshake
%% ===================================================================

-define(BOLT_MAGIC, <<16#60, 16#60, 16#B0, 16#17>>).

-endif.
