-module(neo4j_types).

-include("neo4j.hrl").

-export([
    point_2d/2, point_2d/3, point_3d/3, point_3d/4,
    decode_point/1, encode_point/1,
    decode_date/1, encode_date/1,
    decode_time/1, encode_time/1,
    decode_local_time/1, encode_local_time/1,
    decode_datetime/1, encode_datetime/1,
    decode_local_datetime/1, encode_local_datetime/1,
    decode_duration/1, encode_duration/1,
    advanced_type/1
]).

-define(WGS84_2D, 4326).
-define(WGS84_3D, 4979).
-define(EPOCH_DAYS_OFFSET, 719528). % calendar:date_to_gregorian_days({1970,1,1})

%% ===================================================================
%% Point constructors
%% ===================================================================

point_2d(X, Y) when is_number(X), is_number(Y) ->
    #neo4j_point2d{x = X / 1, y = Y / 1, srid = ?WGS84_2D}.

point_2d(X, Y, Srid) when is_number(X), is_number(Y), is_integer(Srid) ->
    #neo4j_point2d{x = X / 1, y = Y / 1, srid = Srid}.

point_3d(X, Y, Z) when is_number(X), is_number(Y), is_number(Z) ->
    #neo4j_point3d{x = X / 1, y = Y / 1, z = Z / 1, srid = ?WGS84_3D}.

point_3d(X, Y, Z, Srid) when is_number(X), is_number(Y), is_number(Z), is_integer(Srid) ->
    #neo4j_point3d{x = X / 1, y = Y / 1, z = Z / 1, srid = Srid}.

%% ===================================================================
%% Point encode/decode
%% ===================================================================

decode_point([Srid, X, Y]) ->
    #neo4j_point2d{x = X, y = Y, srid = Srid};
decode_point([Srid, X, Y, Z]) ->
    #neo4j_point3d{x = X, y = Y, z = Z, srid = Srid}.

encode_point(#neo4j_point2d{x = X, y = Y, srid = Srid}) ->
    [Srid, X, Y];
encode_point(#neo4j_point3d{x = X, y = Y, z = Z, srid = Srid}) ->
    [Srid, X, Y, Z].

%% ===================================================================
%% Date encode/decode
%% ===================================================================

decode_date([DaysSinceEpoch]) ->
    GregDays = ?EPOCH_DAYS_OFFSET + DaysSinceEpoch,
    {Year, Month, Day} = calendar:gregorian_days_to_date(GregDays),
    #neo4j_date{year = Year, month = Month, day = Day}.

encode_date(#neo4j_date{year = Year, month = Month, day = Day}) ->
    GregDays = calendar:date_to_gregorian_days({Year, Month, Day}),
    DaysSinceEpoch = GregDays - ?EPOCH_DAYS_OFFSET,
    [DaysSinceEpoch].

%% ===================================================================
%% Time encode/decode
%% ===================================================================

decode_time([NanosSinceMidnight, TzOffsetSeconds]) ->
    TotalSeconds = NanosSinceMidnight div 1000000000,
    RemainingNanos = NanosSinceMidnight rem 1000000000,
    Hour = TotalSeconds div 3600,
    Minute = (TotalSeconds rem 3600) div 60,
    Second = TotalSeconds rem 60,
    #neo4j_time{
        hour = Hour,
        minute = Minute,
        second = Second,
        nanosecond = RemainingNanos,
        timezone_offset_seconds = TzOffsetSeconds
    }.

encode_time(#neo4j_time{hour = H, minute = M, second = S,
                        nanosecond = Ns, timezone_offset_seconds = Tz}) ->
    NanosSinceMidnight = H * 3600000000000 + M * 60000000000 + S * 1000000000 + Ns,
    [NanosSinceMidnight, Tz].

%% ===================================================================
%% LocalTime encode/decode
%% ===================================================================

decode_local_time([NanosSinceMidnight]) ->
    TotalSeconds = NanosSinceMidnight div 1000000000,
    RemainingNanos = NanosSinceMidnight rem 1000000000,
    Hour = TotalSeconds div 3600,
    Minute = (TotalSeconds rem 3600) div 60,
    Second = TotalSeconds rem 60,
    #neo4j_local_time{
        hour = Hour,
        minute = Minute,
        second = Second,
        nanosecond = RemainingNanos
    }.

encode_local_time(#neo4j_local_time{hour = H, minute = M, second = S, nanosecond = Ns}) ->
    NanosSinceMidnight = H * 3600000000000 + M * 60000000000 + S * 1000000000 + Ns,
    [NanosSinceMidnight].

%% ===================================================================
%% DateTime encode/decode
%% ===================================================================

decode_datetime([EpochSeconds, Nanosecond, TimezoneId]) when is_binary(TimezoneId) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = epoch_to_datetime(EpochSeconds),
    #neo4j_datetime{
        year = Year, month = Month, day = Day,
        hour = Hour, minute = Minute, second = Second,
        nanosecond = Nanosecond,
        timezone_id = TimezoneId
    };
decode_datetime([EpochSeconds, Nanosecond, TzOffsetSeconds]) when is_integer(TzOffsetSeconds) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = epoch_to_datetime(EpochSeconds),
    TimezoneId = offset_to_string(TzOffsetSeconds),
    #neo4j_datetime{
        year = Year, month = Month, day = Day,
        hour = Hour, minute = Minute, second = Second,
        nanosecond = Nanosecond,
        timezone_id = TimezoneId
    }.

encode_datetime(#neo4j_datetime{year = Y, month = Mo, day = D,
                                hour = H, minute = Mi, second = S,
                                nanosecond = Ns, timezone_id = TzId}) ->
    EpochSeconds = datetime_to_epoch({{Y, Mo, D}, {H, Mi, S}}),
    [EpochSeconds, Ns, TzId].

%% ===================================================================
%% LocalDateTime encode/decode
%% ===================================================================

decode_local_datetime([EpochSeconds, Nanosecond]) ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = epoch_to_datetime(EpochSeconds),
    #neo4j_local_datetime{
        year = Year, month = Month, day = Day,
        hour = Hour, minute = Minute, second = Second,
        nanosecond = Nanosecond
    }.

encode_local_datetime(#neo4j_local_datetime{year = Y, month = Mo, day = D,
                                            hour = H, minute = Mi, second = S,
                                            nanosecond = Ns}) ->
    EpochSeconds = datetime_to_epoch({{Y, Mo, D}, {H, Mi, S}}),
    [EpochSeconds, Ns].

%% ===================================================================
%% Duration encode/decode
%% ===================================================================

decode_duration([Months, Days, Seconds, Nanoseconds]) ->
    #neo4j_duration{months = Months, days = Days, seconds = Seconds, nanoseconds = Nanoseconds}.

encode_duration(#neo4j_duration{months = Mo, days = D, seconds = S, nanoseconds = Ns}) ->
    [Mo, D, S, Ns].

%% ===================================================================
%% Type check
%% ===================================================================

advanced_type(#neo4j_point2d{}) -> true;
advanced_type(#neo4j_point3d{}) -> true;
advanced_type(#neo4j_date{}) -> true;
advanced_type(#neo4j_time{}) -> true;
advanced_type(#neo4j_local_time{}) -> true;
advanced_type(#neo4j_datetime{}) -> true;
advanced_type(#neo4j_local_datetime{}) -> true;
advanced_type(#neo4j_duration{}) -> true;
advanced_type(_) -> false.

%% ===================================================================
%% Internal helpers
%% ===================================================================

epoch_to_datetime(EpochSeconds) ->
    EpochGregSeconds = EpochSeconds + 62167219200, % seconds from year 0 to 1970
    calendar:gregorian_seconds_to_datetime(EpochGregSeconds).

datetime_to_epoch(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime) - 62167219200.

offset_to_string(OffsetSeconds) ->
    AbsOffset = abs(OffsetSeconds),
    Hours = AbsOffset div 3600,
    Minutes = (AbsOffset rem 3600) div 60,
    Sign = if OffsetSeconds >= 0 -> <<"+">>;
              true -> <<"-">>
           end,
    HBin = pad2(Hours),
    MBin = pad2(Minutes),
    <<Sign/binary, HBin/binary, ":", MBin/binary>>.

pad2(N) when N < 10 ->
    iolist_to_binary([$0, integer_to_list(N)]);
pad2(N) ->
    integer_to_binary(N).
