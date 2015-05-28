-module(ts_misc).
-export([build_acyclic_graph/3,all_module_attributes/1,orddict_cons/3]).
-export([list_contain/2]).
-export([to_atom/1,to_binary/1, to_boolean/1,to_float/1,
         to_integer/1,to_integer_2/1,
         to_list/1]).
-export([method_record_type/1]).
-export([add_universal_time/3,compare_date_time/3]).

-export([get_value/2,make_dir/1,nuke_dir/1,split_list/2]).
-export([time_diff/3,trim/1,unique_string/0]).
-export([to_hex/1,to_hex_2/1,to_digit/1,to_bit/1]).
-export([format_hex/1,format_hex_2/1,format_json/1,
         format_json_2/1,
         construct_binary/2,fill_binary_with_value/3,
         construct_list/2,fill_list_with_value/3,
         version_compare/3,
         read_term/1,read_term2/1,write_term/1,
         seed/0,get_id/0,get_id/1,
         floor/1,ceiling/1,
         start_applications/1,stop_applications/1,
         convert_timestamp/1,
         filter_undefined/1,
         df/3,df/2,fv/2,
         check_undefined/2,
         show_command/1,
         check_positive_integer/2,
         check_positive_integer_2/2,
         check_number_2/2,
         check_no_negative_integer_2/2,
         part/2
        ]).

-export([date_to_timestamp/1,timestamp_to_date/1,
         datetime_to_timestamp/1,timestamp_to_datetime/1,
         string_to_date/1,date_to_string/1,
         binary_to_date/1,date_to_binary/1,
         date_to_datetime/1]).

-export([get_uuid_string/0,
        get_mark/0]).

-export([randchar/1,randinteger/1,randemail/0,timestamp/1,
        randdate/0,randhex/1,randfloat/1]).

-define(Q, $\").

%%---------------
%% public api
%%---------------

%%---------------
part(undefined,_)->
               undefined;

part(X,Length) when is_binary(X),is_integer(Length) ->
    Len = size(X),
    case Len =< Length of
        true ->
            X;
        false ->
            binary:part(X,0,Length)
    end;

part(X,Length) when is_list(X),is_integer(Length) ->
    Len = length(X),
    case Len =< Length of
        true ->
            X;
        false ->
            lists:sublist(X,1,Length)
    end.

show_command(Command)->    
    Command_a = print_data(Command),
    Command_a.
    %% lager:debug("command_9999:~n\t~p",[Command_a]).

show_response(Response)->
    Response_a = print_data(Response),
    Response_a.
    %% ?log_debug("response_9999:~n\t~ts",[Response_a]).    

show_response_a(Response)->
    Response_a = print_data(Response),
    Response_a.
    %% ?log_debug("response_9999:~n\t~ts",[Response_a]).   

%%---------------
%% 比~p更好的打印效果
%%---------------
print_data(Data) when is_tuple(Data)->
    Size = size(Data),
    "{" ++ print_data(1,Size,Data) ++ "}";

print_data([]) ->
    "[]";

print_data([Head|Rest]) ->
    "[" ++ print_data(Head) ++ print_data1(Rest) ++ "]";

print_data(Item) when is_binary(Item)->
    lists:flatten(io_lib:format("<<~ts>>",[Item]));

print_data(Item) when is_atom(Item) ->
    lists:flatten(io_lib:format("~ts",[Item]));

print_data(Item) when is_integer(Item) orelse is_float(Item) ->
    lists:flatten(io_lib:format("~p",[Item])).

print_data(Size,Size,Data)->
    Element = element(Size,Data),
    print_data(Element);

print_data(N,Size,Data)->
    Element = element(N,Data),
    print_data(Element) ++ "," ++ print_data(N+1,Size,Data).

print_data1([])->
    "";
    
print_data1([Head|Rest])->
    "," ++ print_data(Head) ++ print_data1(Rest).

test_print_data()->
    test_print_data_a().

test_print_data_a()->    
    show_response_a({aaa}),
    show_response_a([1,2,3]),
    show_response_a({aaa,1,[1,2,3],<<"helloworld">>,<<228,184,141,229,173,152,229,156,168,232,191,153,230,156,172,228,185,166>>}),
    show_response_a({aaa,<<228,184,141,229,173,152,229,156,168,232,191,153,230,156,172,228,185,166>>,
                     bbb}),
    ok.

check_number_2(X,_Reason) when is_number(X),X>0 ->
    ok;
check_number_2(_X,Reason) ->
    throw (Reason).

check_positive_integer_2(X,_Reason) when is_integer(X),X>0 ->
    ok;
check_positive_integer_2(_X,Reason) ->
    throw (Reason).

check_no_negative_integer_2(X,_Reason) when is_integer(X),X>=0 ->
    ok;
check_no_negative_integer_2(_X,Reason) ->
    throw (Reason).

%%---------------
check_undefined(undefined,Reason)->
    throw(Reason);
check_undefined(<<"undefined">>,Reason)->
    throw(Reason);
check_undefined(<<"">>,Reason)->
    throw(Reason);
check_undefined(_,_) ->
    ok.

filter_undefined(List)->
    filter_undefined(List,[]).

filter_undefined([],Result)->
    lists:reverse(Result);

filter_undefined([undefined|Rest],Result)->
    filter_undefined(Rest,Result);
filter_undefined([Other|Rest],Result) ->
    filter_undefined(Rest,[Other|Result]).

%% Execute Fun using the IO system of the local node (i.e. the node on
%% which the code is executing).
with_local_io(Fun) ->
    GL = group_leader(),
    group_leader(whereis(user), self()),
    try
        Fun()
    after
        group_leader(GL, self())
    end.

%% Log an info message on the local node using the standard logger.
%% Use this if rabbit isn't running and the call didn't originate on
%% the local node (e.g. rabbitmqctl calls).
local_info_msg(Format, Args) ->
    with_local_io(fun () -> error_logger:info_msg(Format, Args) end).

seed()->
    {M_a,M_b,M_c} = now(),
    random:seed(M_a,M_b,M_c),
    ok.

%%--------------
%% 生成随机26个字符的字符串
%%--------------
randchar(N) ->
   randchar(N, []).

randchar(0, Acc) ->
   love_misc:to_binary(Acc);
randchar(N, Acc) ->
   randchar(N - 1, [random:uniform(26) + 96 | Acc]).

%%-------------
%% 生成随机固定长度的整数字符串
%%-------------
randinteger(N) ->
   randinteger(N, []).

randinteger(0, Acc) ->
   love_misc:to_binary(Acc);
randinteger(N, Acc) ->
   randinteger(N - 1, [random:uniform(10) + 47 | Acc]).

%%------------
%% 生成随机邮件地址
%%------------
randemail()->
    Name = randchar(10),
    <<Name/binary, "@qq.com">>.

%%------------
%% 生成随机日期
%%------------
randdate()->
    Timestamp = random:uniform(timestamp(now())),
    D= {Timestamp div 1000000000000, 
                            Timestamp div 1000000 rem 1000000,
                            Timestamp rem 1000000},
    timestamp_to_date(D).

timestamp({Mega, Secs, Micro}) ->
    Mega*1000*1000*1000*1000 + Secs * 1000 * 1000 + Micro.

%%-----------
%% 生成随机的hex字符串
%%-----------
randhex(N) ->
   randhex(N*2, []).

randhex(0, Acc) ->
   love_misc:to_binary(Acc);
randhex(N, Acc) ->
    V = case (Value = random:uniform(16)) =< 10 of
        true ->
            47 + Value;
        false ->
            54 + Value                
    end,
   randhex(N - 1, [V | Acc]).

randfloat(N)->
    {V,_} = random:uniform_s(now()),
    V * N.

%%-----------
%%
%%-----------
get_id(1)-> random:uniform(1);
get_id(2) -> random:uniform(10);
get_id(3) -> random:uniform(100);
get_id(4) -> random:uniform(1000);
get_id(5) -> random:uniform(10000);
get_id(6) -> random:uniform(100000);
get_id(7) -> random:uniform(1000000);
get_id(8) -> random:uniform(10000000);
get_id(9) -> random:uniform(100000000);
get_id(10) -> random:uniform(1000000000);
get_id(11) -> random:uniform(10000000000);
get_id(12) -> random:uniform(100000000000);
get_id(13) -> random:uniform(1000000000000);
get_id(14) -> random:uniform(10000000000000);
get_id(15) -> random:uniform(100000000000000);
get_id(16) -> random:uniform(1000000000000000);
get_id(17) -> random:uniform(10000000000000000);
get_id(18) -> random:uniform(100000000000000000);
get_id(19) -> random:uniform(1000000000000000000);
get_id(20) -> random:uniform(10000000000000000000).

get_id()-> random:uniform(10000000).

get_mark()->
    get_uuid_string().

get_value(Key, List) ->
    get_value(Key, List, undefined).

get_value(Key, List, Default) ->
    case lists:keysearch(Key, 1, List) of
    {value, {Key,Value}} ->
        Value;
    false ->
        Default
    end.

ensure_ok(ok, _) -> ok;
ensure_ok({error, Reason}, ErrorTag) -> throw({error, {ErrorTag, Reason}}).

execute_mnesia_transaction(TxFun) ->
    %% Making this a sync_transaction allows us to use dirty_read
    %% elsewhere and get a consistent result even when that read
    %% executes on a different node.
    %%    case worker_pool:submit(
    %%    fun () ->
    Result_a = case mnesia:is_transaction() of
                   false -> DiskLogBefore = mnesia_dumper:get_log_writes(),
                            Res = mnesia:sync_transaction(TxFun),
                            DiskLogAfter  = mnesia_dumper:get_log_writes(),
                            case DiskLogAfter == DiskLogBefore of
                                true  -> Res;
                                false -> {sync, Res}
                            end;
                   true  -> 
                       mnesia:sync_transaction(TxFun)                     
               end,
    case Result_a of
        {sync, {atomic,  Result}} -> mnesia_sync:sync(), Result;
        {sync, {aborted, Reason}} -> throw({error, Reason});
        {atomic,  Result}         -> Result;
        {aborted, Reason}         -> throw({error, Reason})
    end.

dirty_read({Table, Key}) ->
    case ets:lookup(Table, Key) of
        [Result] -> {ok, Result};
        []       -> {error, not_found};
        Data -> {ok, Data}
    end.

format(Fmt, Args) -> lists:flatten(io_lib:format(Fmt, Args)).

build_acyclic_graph(VertexFun, EdgeFun, Graph) ->
    G = digraph:new([acyclic]),
    try
        [case digraph:vertex(G, Vertex) of
             false -> digraph:add_vertex(G, Vertex, Label);
             _     -> ok = throw({graph_error, {vertex, duplicate, Vertex}})
         end || {Module, Atts}  <- Graph,
                {Vertex, Label} <- VertexFun(Module, Atts)],
        [case digraph:add_edge(G, From, To) of
             {error, E} -> throw({graph_error, {edge, E, From, To}});
             _          -> ok
         end || {Module, Atts} <- Graph,
                {From, To}     <- EdgeFun(Module, Atts)],
        {ok, G}
    catch {graph_error, Reason} ->
            true = digraph:delete(G),
            {error, Reason}
    end.

all_module_attributes(Name) ->
    Modules =
        lists:usort(
          lists:append(
            [Modules || {App, _, _}   <- application:loaded_applications(),
                        {ok, Modules} <- [application:get_key(App, modules)]])),
    lists:foldl(
      fun (Module, Acc) ->
              case lists:append([Atts || {N, Atts} <- module_attributes(Module),
                                         N =:= Name]) of
                  []   -> Acc;
                  Atts -> [{Module, Atts} | Acc]
              end
      end, [], Modules).

orddict_cons(Key, Value, Dict) ->
    orddict:update(Key, fun (List) -> [Value | List] end, [Value], Dict).

module_attributes(Module) ->
    case catch Module:module_info(attributes) of
        {'EXIT', {undef, [{Module, module_info, _} | _]}} ->
            io:format("WARNING: module ~p not found, so not scanned for boot steps.~n",
                      [Module]),
            [];
        {'EXIT', Reason} ->
            exit(Reason);
        V ->
            V
    end.

list_contain([],_)->
    false;

list_contain([Element|_],Element)->
    true;

list_contain([_|Rest],Element) ->
    list_contain(Rest,Element).  




% removes leading and trailing whitespace from a string
trim(String) ->
    String2 = lists:dropwhile(fun is_whitespace/1, String),
    lists:reverse(lists:dropwhile(fun is_whitespace/1, lists:reverse(String2))).

to_binary(undefined)->
    undefined;

to_binary(V) when is_binary(V) ->
    V;
to_binary(V) when is_list(V) ->
    try
        list_to_binary(V)
    catch
        _:_ ->
            list_to_binary(io_lib:format("~p", [V]))
    end;
to_binary(V) when is_atom(V) ->
    list_to_binary(atom_to_list(V));
to_binary(V) ->
    list_to_binary(io_lib:format("~p", [V])).

%%------------
%% 兼容undefined的情况
%%------------
to_integer_2(undefined)->
    undefined;
to_integer_2(V) ->
    to_integer(V).

%%------------
%% 不兼容undefined的情况
%%------------
to_integer(V) when is_integer(V) ->
    V;
to_integer(V) when is_list(V) ->
    erlang:list_to_integer(V);
to_integer(V) when is_binary(V) ->
    erlang:list_to_integer(binary_to_list(V));
to_integer(V) when is_atom(V)->
    erlang:list_to_integer(atom_to_list(V)).    

to_list(V) when is_list(V) ->
    V;
to_list(V) when is_binary(V) ->
    binary_to_list(V);
to_list(V) when is_atom(V) ->
    atom_to_list(V);
to_list(V) ->
    lists:flatten(io_lib:format("~p", [V])).

to_atom(V) when is_atom(V)->
    V;
to_atom(V) when is_list(V) ->
    list_to_atom(V);
to_atom(V) when is_binary(V)->
    list_to_atom(binary_to_list(V));
to_atom(V) ->
    list_to_atom(lists:flatten(io_lib:format("~p", [V]))).

to_boolean(Data) when is_binary(Data) ->
    A = list_to_atom(to_list(Data)),
    true = is_boolean(A),
    A;

to_boolean(Data) when is_boolean(Data) ->
    Data;
to_boolean(Data) when is_list(Data) ->
    A = list_to_atom(Data),
    true = is_boolean(A),
    A.

to_float(X) when is_integer(X)->
    erlang:float(X);

to_float(Data) when is_float(Data)->
    Data;

to_float(Data) when is_binary(Data) ->
    N = binary_to_list(Data),
    case string:to_float(N) of
        {error,no_float} -> list_to_integer(N);
        {F,_Rest} -> F
    end;
%% A = binary_to_float(Data),
%% A;

to_float(Data) when is_list(Data) ->
    A = list_to_float(Data),
    A.

method_record_type(Record) ->
    element(1, Record).

%% capulet_error(Name, ExplanationFormat, Params, Method) ->
%%     Explanation = format(ExplanationFormat, Params),
%%     #capulet_error{name = Name, explanation = Explanation, method = Method}.

%% protocol_error(Name, ExplanationFormat, Params) ->
%%     protocol_error(Name, ExplanationFormat, Params, none).

%% protocol_error(Name, ExplanationFormat, Params, Method) ->
%%     protocol_error(capulet_error(Name, ExplanationFormat, Params, Method)).

%% protocol_error(#capulet_error{} = Error) ->
%%     exit(Error).

%%================
%%进行日期比较
compare_date_time(datetime,First,Second)->
    First_time = calendar:datetime_to_gregorian_seconds(First),
    Second_time = calendar:datetime_to_gregorian_seconds(Second),
    compare(First_time - Second_time).
    
%%返回-1,表示前者时间晚
compare(Result) when Result > 0 ->
    -1;
%%================
%%返回1,表示前者时间早
compare(Result) when Result < 0 ->
    1;
compare(_)  ->
    0.

%%=================
%%时间运算
add_universal_time(minute,Interval,Date_time)->
     Second_interval = Interval * 60 ,
    add_universal_time(second,Second_interval,Date_time);   

add_universal_time(hour,Interval,Date_time) ->
     Second_interval = Interval * 60 * 60,
    add_universal_time(second,Second_interval,Date_time);   

add_universal_time(day,Interval,Date_time)->
    Second_interval = Interval * 60 * 60 * 24,
    add_universal_time(second,Second_interval,Date_time);

add_universal_time(month,Interval,Date_time)->
    {Date,Time} = Date_time,
    New_date = edate:shift(Date,Interval,months),
    New_date_time = {New_date,Time},
    New_date_time;

add_universal_time(second,Second_interval,Date_time)->
    Seconds = calendar:datetime_to_gregorian_seconds(Date_time),
    New_seconds = Seconds + Second_interval,
    New_date_time = calendar:gregorian_seconds_to_datetime(New_seconds),
    New_date_time.

time_diff(datetime,T2,T1)->
    S2 = datetime_to_timestamp(T2),
    S1 = datetime_to_timestamp(T1),
    timer:now_diff(S2,S1).

%%==================
%% 目录删除
%%==================
nuke_dir(Dir) ->
    FoldFun = fun(File) ->
        Path = filename:join(Dir, File),
        case file:delete(Path) of
            {error, eperm} -> ok = nuke_dir(Path);
            {error, enoent} -> ok;
            ok -> ok
        end
    end,
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:foreach(FoldFun, Files),
            ok = file:del_dir(Dir);
        {error, enoent} ->
            ok
    end.


make_dir(Local_path)->
    Result = file:make_dir(Local_path) ,
    error_logger:info_report([scm_client_tests_make_dir_1,Local_path,Result]),
    case Result of
        ok ->
            ok;
        {error,eexist} ->
            ok;
        _ ->
            error
    end.

%%=========================
%% 拆分list的方法
%%=========================    
split_list(Size,List)->
    split_list(Size,List,[]).

split_list(Size,List,Group) when Size >= length(List) ->
    lists:reverse([List|Group]);

split_list(Size,List,Group) ->
    A = lists:sublist(List,Size),
    Rest = lists:nthtail(Size,List),
    split_list(Size,Rest,[A|Group]).

%%==================
%%随机字符串生成
unique_string()->
    %%  base64:encode_to_string(term_to_binary(make_ref())).
    love_misc:to_hex(love_misc:to_list(term_to_binary(make_ref()))).

%%======================================
%% internal API
%%======================================
datetime_to_timestamp(Date_time)->    
    Seconds = calendar:datetime_to_gregorian_seconds(Date_time) - 62167219200,
    %% 62167219200 == calendar:datetime_to_gregorian_seconds(
    %% {{1970, 1, 1}, {0, 0, 0}})
    {Seconds div 1000000, Seconds rem 1000000, 0}.

timestamp_to_datetime({H,M,L})-> 
    Seconds = H * 1000000 + M  + 62167219200,
    calendar:gregorian_seconds_to_datetime(Seconds).
    
date_to_timestamp(Date)->    
    Seconds = calendar:datetime_to_gregorian_seconds({Date,{0,0,0}}) - 62167219200,
    {Seconds div 1000000, Seconds rem 1000000, 0}.
timestamp_to_date({H,M,L})-> 
    Seconds = H * 1000000 + M + 62167219200,
    {Date,{_,_,_}}= calendar:gregorian_seconds_to_datetime(Seconds),
    Date.

string_to_date(undefined)->
    undefined;
string_to_date(Other) ->
    edate:string_to_date(Other).

date_to_string(undefined)->
    undefined;
date_to_string(Other) ->
    edate:date_to_string(Other).

%%----------------
%% 日期转换为binary
%%----------------
binary_to_date(undefined)->
    undefined;
binary_to_date(Other) ->
    edate:string_to_date(love_misc:to_list(Other)).

date_to_binary(undefined)->
    undefined;
date_to_binary(Other) ->
    love_misc:to_binary(edate:date_to_string(Other)).

% Is a character whitespace?
is_whitespace($\s) -> true;
is_whitespace($\t) -> true;
is_whitespace($\n) -> true;
is_whitespace($\r) -> true;
is_whitespace(_Else) -> false.
    

to_hex([]) ->
    [];
to_hex(Bin) when is_binary(Bin) ->
    to_hex(binary_to_list(Bin));
to_hex([H|T]) ->
    [to_digit(H div 16), to_digit(H rem 16) | to_hex(T)].

%%=============
%% 从16进制字符串的binary转换为16进制的数字的binary
%%=============
to_hex_2(<<"undefined">>)->
    <<>>; 
to_hex_2(X) when is_binary(X)->
    to_hex_2(X,<<>>);
to_hex_2(_)->
    <<>>.

to_hex_2(<<>>,Result)->
    Result;
%% to_hex_2(<<A:8/integer,B:8/integer,Rest/binary>>,Result) ->
%%     C = (A - $0)*16 + (B-$0),
%%     D = <<Result/binary,C:8/integer>>,
%%     to_hex_2(Rest,D).
to_hex_2(<<A:8/integer,B:8/integer,Rest/binary>>,Result) ->
    C = list_to_integer([A,B],16),
    D = <<Result/binary,C:8/integer>>,
    %%lager:info("love_misc:to_hex_2,~p,~p,~p,~p",[A,B,C,D]),
    to_hex_2(Rest,D).


to_digit(N) when N < 10 -> $0 + N;
to_digit(N)             -> $a + N-10.

format_hex(Binary) when is_binary(Binary)->
    lists:flatten(
      io_lib:format("<<~s>>", 
                    [[io_lib:format("~2.16.0B",[X]) || <<X:8>> <= Binary ]])).

format_hex_2(Binary) when is_binary(Binary)->
    A = lists:flatten(
      io_lib:format("~s", 
                    [[io_lib:format("~2.16.0B",[X]) || <<X:8>> <= Binary ]])),
    to_binary(A);
format_hex_2(_)->
    <<>>.

to_bit(Binary) when is_binary(Binary)->
    lists:flatten(
      io_lib:format("<<~s>>", 
                    [[io_lib:format("~8.2.0B",[X]) || <<X:8>> <= Binary ]])).
%%     to_bit(Binary,[]).

%% to_bit(<<>>,Result)->
%%     A = lists:reverse(Result),
%%     lists:flatten(A);

%% to_bit(<<A:8/integer,Rest/binary>>,Result)->
%%     B = io_lib:format("~8.2.0B",[A]),
%%     error_logger:info_report([love_misc_to_bit_1,B]),
%%     to_bit(Rest, B ++ Result).
    

%%================
%% 输出日志
%%================
%% format_json(A) when A == [] ->
%%     error_logger:format("{}");

format_json(A) when is_list(A)->
    %%error_logger:format("~s~n",[lists:flatten(A)]).
    B = lists:flatten(A),
    love_misc:to_binary(B).

format_json_2(A) ->
    lists:flatten(love_misc:to_list(A)).

%%==================
%% 构造binary数据
%%==================
construct_binary(Padding,Length) when is_binary(Padding) 
                                      andalso (size(Padding) == 1)->
    List = lists:seq(1,Length),
    lists:foldl(fun(_X,B)->
                        <<Padding/binary,B/binary>> end,<<>>,List).

construct_list(Padding,Length) when Length >= 0 ->
    lists:map(fun(_X)->Padding end,lists:seq(1,Length)).
    
%%==================
%% 对于binary补齐多余数据
%%==================
fill_binary_with_value(Length,_Padding,Binary_data) 
  when size(Binary_data) >= Length ->
    Binary_data;
fill_binary_with_value(Length,Padding,Binary_data)
  when size(Binary_data) < Length andalso is_binary(Padding) ->
    Left = (Length - size(Binary_data)),
    Left_binary = construct_binary(Padding,Left),
    <<Binary_data/binary,Left_binary/binary>>.

fill_list_with_value(Length,_Padding,List_data) 
  when length(List_data) >= Length ->
    List_data;
fill_list_with_value(Length,Padding,List_data) 
  when length(List_data) < Length
       ->
    Left = Length - length(List_data),  
    Left_list = construct_list(Padding,Left),
    List_data ++ Left_list.

%%=============
%% 版本比较
%%=============
version_compare(A, B, lte) ->
    case version_compare(A, B) of
        eq -> true;
        lt -> true;
        gt -> false
    end;
version_compare(A, B, gte) ->
    case version_compare(A, B) of
        eq -> true;
        gt -> true;
        lt -> false
    end;
version_compare(A, B, Result) ->
    Result =:= version_compare(A, B).

version_compare(A, A) ->
    eq;
version_compare([], [$0 | B]) ->
    version_compare([], dropdot(B));
version_compare([], _) ->
    lt; %% 2.3 < 2.3.1
version_compare([$0 | A], []) ->
    version_compare(dropdot(A), []);
version_compare(_, []) ->
    gt; %% 2.3.1 > 2.3
version_compare(A,  B) ->
    {AStr, ATl} = lists:splitwith(fun (X) -> X =/= $. end, A),
    {BStr, BTl} = lists:splitwith(fun (X) -> X =/= $. end, B),
    ANum = list_to_integer(AStr),
    BNum = list_to_integer(BStr),
    if ANum =:= BNum -> version_compare(dropdot(ATl), dropdot(BTl));
       ANum < BNum   -> lt;
       ANum > BNum   -> gt
    end.

dropdot(A) -> lists:dropwhile(fun (X) -> X =:= $. end, A).

%%======================
%% 解析数据
%%======================
read_term2(undefined)->
    ok;

read_term2(Data) ->
    try
        %% {ok, Data} = with_fhc_handle(fun () -> prim_file:read_file(File) end),
        {ok, Tokens, _} = erl_scan:string(binary_to_list(Data)),
        TokenGroups = group_tokens(Tokens),
        %% [TokenGroup] = group_tokens(Tokens),
        {ok, [begin
                  {ok, Term} = erl_parse:parse_term(Tokens1),
                  Term
              end || Tokens1 <- TokenGroups]}
        %% {ok, Term} = erl_parse:parse_term(TokenGroups),
        %% Term
        %% {ok,begin
        %%         {ok, Term} = erl_parse:parse_term(TokensGroups),
        %%         Term
        %%     end}
    catch
        error:{badmatch, Error} -> Error
    end.

read_term(Data) ->
    try
        %% {ok, Data} = with_fhc_handle(fun () -> prim_file:read_file(File) end),
        {ok, Tokens, _} = erl_scan:string(binary_to_list(Data)),
        TokenGroups = group_tokens(Tokens),
        %% [TokenGroup] = group_tokens(Tokens),
        {ok, [begin
                  {ok, Term} = erl_parse:parse_term(Tokens1),
                  Term
              end || Tokens1 <- TokenGroups]}
        %% {ok, Term} = erl_parse:parse_term(TokenGroups),
        %% Term
        %% {ok,begin
        %%         {ok, Term} = erl_parse:parse_term(TokensGroups),
        %%         Term
        %%     end}
    catch
        error:{badmatch, Error} -> Error
    end.

group_tokens(Ts) -> [lists:reverse(G) || G <- group_tokens([], Ts)].

group_tokens([], [])                    -> [];
group_tokens(Cur, [])                   -> [Cur];
group_tokens(Cur, [T = {dot, _} | Ts])  -> [[T | Cur] | group_tokens([], Ts)];
group_tokens(Cur, [T | Ts])             -> group_tokens([T | Cur], Ts).

write_term(Data) when is_list(Data) ->
    list_to_binary([io_lib:format("~w.~n",[Term])||
                               Term <- Data]);

write_term(Data)->
    list_to_binary(io_lib:format("~w.~n",[Data])).


floor(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        N when N < 0 -> T - 1;
        N when N > 0 -> T;
        _            -> T
    end.

ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        N when N < 0 -> T;
        N when N > 0 -> T + 1;
        _            -> T
    end.

manage_applications(Iterate, Do, Undo, SkipError, ErrorTag, Apps) ->
    Iterate(fun (App, Acc) ->
                    %% lager:info("love_misc_manage_application_1:~p,~p",
                    %%            [App,Acc]),
                    case Do(App) of
                        ok -> [App | Acc];
                        {error, {SkipError, _}} -> Acc;
                        {error, Reason} ->
                            lists:foreach(Undo, Acc),
                            throw({error, {ErrorTag, App, Reason}})
                    end
            end, [], Apps),
    ok.

start_applications(Apps) ->
    manage_applications(fun lists:foldl/3,
                        fun application:start/1,
                        fun application:stop/1,
                        already_started,
                        cannot_start_application,
                        Apps).

stop_applications(Apps) ->
    manage_applications(fun lists:foldr/3,
                        fun application:stop/1,
                        fun application:start/1,
                        not_started,
                        cannot_stop_application,
                        Apps).

convert_timestamp(Datetime)->
    Seconds = calendar:datetime_to_gregorian_seconds(Datetime) - 62167219200,
    {Seconds div 1000000, Seconds rem 1000000, 0}.

%%---------------------
%% json_encode_string(A, State) when is_atom(A) ->
%%     L = atom_to_list(A),
%%     case json_string_is_safe(L) of
%%         true ->
%%             [?Q, L, ?Q];
%%         false ->
%%             json_encode_string_unicode(xmerl_ucs:from_utf8(L), State, [?Q])
%%     end;
%% json_encode_string(B, State) when is_binary(B) ->
%%     case json_bin_is_safe(B) of
%%         true ->
%%             [?Q, B, ?Q];
%%         false ->
%%             json_encode_string_unicode(xmerl_ucs:from_utf8(B), State, [?Q])
%%     end;
%% json_encode_string(I, _State) when is_integer(I) ->
%%     [?Q, integer_to_list(I), ?Q];
%% json_encode_string(L, State) when is_list(L) ->
%%     case json_string_is_safe(L) of
%%         true ->
%%             [?Q, L, ?Q];
%%         false ->
%%             json_encode_string_unicode(L, State, [?Q])
%%     end.

%% json_string_is_safe([]) ->
%%     true;
%% json_string_is_safe([C | Rest]) ->
%%     case C of
%%         ?Q ->
%%             false;
%%         $\\ ->
%%             false;
%%         $\b ->
%%             false;
%%         $\f ->
%%             false;
%%         $\n ->
%%             false;
%%         $\r ->
%%             false;
%%         $\t ->
%%             false;
%%         C when C >= 0, C < $\s; C >= 16#7f, C =< 16#10FFFF ->
%%             false;
%%         C when C < 16#7f ->
%%             json_string_is_safe(Rest);
%%         _ ->
%%             false
%%     end.

%% json_bin_is_safe(<<>>) ->
%%     true;
%% json_bin_is_safe(<<C, Rest/binary>>) ->
%%     case C of
%%         ?Q ->
%%             false;
%%         $\\ ->
%%             false;
%%         $\b ->
%%             false;
%%         $\f ->
%%             false;
%%         $\n ->
%%             false;
%%         $\r ->
%%             false;
%%         $\t ->
%%             false;
%%         C when C >= 0, C < $\s; C >= 16#7f ->
%%             false;
%%         C when C < 16#7f ->
%%             json_bin_is_safe(Rest)
%%     end.


json_encode_string_unicode(L) 
  when is_binary(L)->
    L;

json_encode_string_unicode(L)->
    json_encode_string_unicode(L,[]).

json_encode_string_unicode([],Acc) ->
    %% lists:reverse([$\" | Acc]);
    to_binary(lists:reverse(Acc));
json_encode_string_unicode([C | Cs],Acc) ->
    Acc1 = case C of
               ?Q ->
                   [?Q, $\\ | Acc];
               %% Escaping solidus is only useful when trying to protect
               %% against "</script>" injection attacks which are only
               %% possible when JSON is inserted into a HTML document
               %% in-line. mochijson2 does not protect you from this, so
               %% if you do insert directly into HTML then you need to
               %% uncomment the following case or escape the output of encode.
               %%
               %% $/ ->
               %%    [$/, $\\ | Acc];
               %%
               $\\ ->
                   [$\\, $\\ | Acc];
               $\b ->
                   [$b, $\\ | Acc];
               $\f ->
                   [$f, $\\ | Acc];
               $\n ->
                   [$n, $\\ | Acc];
               $\r ->
                   [$r, $\\ | Acc];
               $\t ->
                   [$t, $\\ | Acc];
               ${ ->
                  %% [${,$\\ |Acc];
                   Acc;
               $} ->
                  %% [$},$\\ |Acc];
                   Acc;
               $[ ->
                  %% [$[,$\\ |Acc];
                   Acc;
               $] ->
                   %%[$],$\\ |Acc];
                   Acc;
               $: ->
                  %% [$:,$\\ |Acc];
                   Acc;
               C when C >= 0, C < $\s ->
                   [unihex(C) | Acc];
               C when C >= 16#7f, C =< 16#10FFFF ->
                   [xmerl_ucs:to_utf8(C) | Acc];
               C when  C >= 16#7f, C =< 16#10FFFF ->
                   [unihex(C) | Acc];
               C when C < 16#7f ->
                   [C | Acc];
               _ ->
                   exit({json_encode, {bad_char, C}})
           end,
    json_encode_string_unicode(Cs,Acc1).

hexdigit(C) when C >= 0, C =< 9 ->
    C + $0;
hexdigit(C) when C =< 15 ->
    C + $a - 10.

unihex(C) when C < 16#10000 ->
    <<D3:4, D2:4, D1:4, D0:4>> = <<C:16>>,
    Digits = [hexdigit(D) || D <- [D3, D2, D1, D0]],
    [$\\, $u | Digits];
unihex(C) when C =< 16#10FFFF ->
    N = C - 16#10000,
    S1 = 16#d800 bor ((N bsr 10) band 16#3ff),
    S2 = 16#dc00 bor (N band 16#3ff),
    [unihex(S1), unihex(S2)].

get_uuid_string()->
    love_misc:to_binary(uuid:to_string(uuid:v4())).

%% parse_uuid_string(undefined)->
%%     undefined;
%% parse_uuid_string(Other) ->
    
df(X,Y)->
    case dict:fetch(X,Y) of
        "undefined" ->
            undefined;
        <<"undefined">> ->
            undefined;
        "null" ->
            undefined;
        <<"null">> ->
            undefined;
        Other ->
            Other
    end.

df(X,Y,Type)->
    case dict:fetch(X,Y) of
        "undefined" ->
            undefined;
        <<"undefined">> ->
            undefined;
        "null" ->
            undefined;
        <<"null">> ->
            undefined;
        Other ->
            case Type of
                datetime ->
                    parse_datetime(Other);
                date ->
                    parse_date(Other);
                binary ->
                    love_misc:to_binary(Other);
                atom ->
                    love_misc:to_atom(Other);
                integer ->
                    love_misc:to_integer(Other);
                float ->
                    love_misc:to_float(Other);
                boolean ->
                    love_misc:to_boolean(Other);
                Unknow ->
                    {error,unknow_type,Unknow}
            end            
    end.

fv(<<"undefined">>,_)->
    undefined;

fv(Other,Type)->
    case Type of
        datetime ->
            parse_datetime(Other);
        date ->
            parse_date(Other);
        binary ->
            love_misc:to_binary(Other);
        atom ->
            love_misc:to_atom(Other);
        integer ->
            love_misc:to_integer(Other);
        float ->
            love_misc:to_float(Other);
        Unknow ->
            {error,unknow_type,Unknow}
    end.

parse_datetime(<<"defined">>)->
    undefined;

parse_datetime(Other) ->
    iso8601:parse(Other).


parse_date(<<"defined">>)->
    undefined;

parse_date(Other) when is_binary(Other) ->
    %% iso8601:parse(Other).
    edate:string_to_date(love_misc:to_list(Other));

parse_date(Other) when is_list(Other) ->
    %% iso8601:parse(Other).
    edate:string_to_date(Other).

date_to_datetime(Date)->
	{Date,{0,0,0}}.

%%--------------
%% 正整数检查
%%--------------
check_positive_integer(undefined,_)->
    ok;
check_positive_integer(Value,_Error) when is_integer(Value), Value > 0 ->
    ok;
check_positive_integer(_,Error) ->
    throw(Error).
