%%
%% %CopyrightBegin%
%% 
%% Copyright Ericsson AB 1998-2011. All Rights Reserved.
%% 
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%% 
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%% 
%% %CopyrightEnd%
%%
-module(s_group).

%% Groups nodes into s_groups with an own global name space.

-behaviour(gen_server).

%% External exports
-export([start/0, start_link/0, stop/0, init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2, 
	 terminate/2, code_change/3]).

-export([s_groups/0]).
-export([monitor_nodes/1]).
-export([own_nodes/0]).
-export([registered_names/1]).
-export([send/2]).
-export([send/3]).
-export([whereis_name/1]).
-export([whereis_name/2]).
-export([sync/0]).
-export([ng_add_check/2, ng_add_check/3]).

-export([info/0]).
-export([registered_names_test/1]).
-export([send_test/2]).
-export([whereis_name_test/1]).
-export([get_own_nodes/0, get_own_nodes_with_errors/0,
         get_own_s_groups_with_nodes/0]).
-export([publish_on_nodes/0]).

-export([config_scan/1, config_scan/2]).

-export([new_s_group/2, remove_s_group/1,
         s_group_add_nodes/2,
         s_group_remove_nodes/2,
         new_s_group_check/3,
         remove_s_group_check/1,
         s_group_add_nodes_check/3,
         s_group_remove_nodes_check/2]).

%% Internal exports
-export([sync_init/4]).

-define(cc_vsn, 2).

%%-define(debug(_), ok).

-define(debug(T), erlang:display({node(), {line,?LINE}, T})).
%%%====================================================================================

-type publish_type() :: 'hidden' | 'normal'.
-type sync_state()   :: 'no_conf' | 'synced'.

-type group_name()  :: atom().
-type group_tuple() :: {GroupName :: group_name(), [node()]}
                     | {GroupName :: group_name(),
                        PublishType :: publish_type(),
                        [node()]}.

%%%====================================================================================
%%% The state of the s_group process
%%% 
%%% sync_state =  no_conf (s_groups not defined, inital state) |
%%%               synced 
%%% group_name =  Own global group name
%%% nodes =       Nodes in the own global group
%%% no_contact =  Nodes which we haven't had contact with yet
%%% sync_error =  Nodes which we haven't had contact with yet
%%% other_grps =  list of other global group names and nodes, [{otherName, [Node]}]
%%% node_name =   Own node 
%%% monitor =     List of Pids requesting nodeup/nodedown
%%%====================================================================================

-record(state, {sync_state = no_conf        :: sync_state(),
		connect_all                 :: boolean(),
		group_names = []            :: [group_name()],  %% type changed by HL;
		nodes = []                  :: [node()],
		no_contact = []             :: [node()],
		sync_error = []             :: [node()],
		other_grps = []             :: [{group_name(), [node()]}],
                own_grps =[]                :: [{group_name(), [node()]}], %% added by HL;
		node_name = node()          :: node(),
		monitor = []                :: [pid()],
		publish_type = normal       :: publish_type(),    %% node  default pubtype is normal
                group_publish_type = hidden :: publish_type()}).  %% all s_groups are hidden?



%%%====================================================================================
%%% External exported
%%%====================================================================================

-spec s_groups() ->  {GroupName, GroupNames}  | undefined when
              GroupName ::[group_name()], GroupNames :: [GroupName].
s_groups() ->
    request(s_groups).

-spec monitor_nodes(Flag) -> 'ok' when
      Flag :: boolean().
monitor_nodes(Flag) -> 
    case Flag of
	true -> request({monitor_nodes, Flag});
	false -> request({monitor_nodes, Flag});
	_ -> {error, not_boolean}
    end.

-spec own_nodes() -> Nodes when
      Nodes :: [Node :: node()].
own_nodes() ->
    request(own_nodes).

-type name()  :: atom().
-type where() :: {'node', node()} | {'group', group_name()}.

-spec registered_names(Where) -> Names when
      Where :: where(),
      Names :: [Name :: name()].
registered_names(Arg) ->
    request({registered_names, Arg}).

-spec send(Name, Msg) -> pid() | {'badarg', {Name, Msg}} when
      Name :: name(),
      Msg :: term().
send(Name, Msg) ->
    request({send, Name, Msg}).

-spec send(Where, Name, Msg) -> pid() | {'badarg', {Name, Msg}} when
      Where :: where(),
      Name :: name(),
      Msg :: term().
send(Group, Name, Msg) ->
    request({send, Group, Name, Msg}).

-spec whereis_name(Name) -> pid() | 'undefined' when
      Name :: name().
whereis_name(Name) ->
    request({whereis_name, Name}).

-spec whereis_name(Where, Name) -> pid() | 'undefined' when
      Where :: where(),
      Name :: name().
whereis_name(Group, Name) ->
    request({whereis_name, Group, Name}).

-spec sync() -> 'ok'.
sync() ->
    request(sync).

ng_add_check(Node, OthersNG) ->
    ng_add_check(Node, normal, OthersNG).

ng_add_check(Node, PubType, OthersNG) ->
    request({ng_add_check, Node, PubType, OthersNG}).

-type info_item() :: {'state', State :: sync_state()}
                   | {'own_group_names', GroupName :: [group_name()]}
                   | {'own_group_nodes', Nodes :: [node()]}
                   | {'synched_nodes', Nodes :: [node()]}
                   | {'sync_error', Nodes :: [node()]}
                   | {'no_contact', Nodes :: [node()]}
                   | {'own_groups', OwnGroups::[group_tuple()]}
                   | {'other_groups', Groups :: [group_tuple()]}
                   | {'monitoring', Pids :: [pid()]}
                   | {'publish_type', PubType:: publish_type()}
                   | {'group_publish__type', PubType:: publish_type()}.

-spec info() -> [info_item()].
info() ->
    request(info, 3000).

-spec new_s_group(GroupName::group_name(), Nodes::[node()]) ->
                         ok|{error, Reason::term()}.
new_s_group(GroupName, Nodes) ->
    case request({new_s_group, GroupName, Nodes}) of 
        ok ->
            ok=request({new_s_group_1, GroupName, Nodes}),
            timer:sleep(1000),
             sync();
        {error, Reason} ->
            Reason
    end.

new_s_group_check(Node, PubType, {GroupName, Nodes}) ->
    request({new_s_group_check, Node, PubType, {GroupName, Nodes}}).

-spec remove_s_group(GroupName::group_name()) -> ok | {error, Reason::term()}.
remove_s_group(GroupName) ->
    request({remove_s_group, GroupName}).

remove_s_group_check(GroupName) ->
    request({remove_s_group_check, GroupName}).

-spec s_group_add_nodes(GroupName::group_name(), Nodes::[node()]) ->
                               {ok, [node()]} | {error, Reason::term()}.

s_group_add_nodes(GroupName, Nodes) -> 
    request({s_group_add_nodes, GroupName, Nodes}).

s_group_add_nodes_check(Node, PubType, {GroupName, Nodes}) ->
    request({s_group_add_nodes_check, Node, PubType, {GroupName, Nodes}}).


-spec s_group_remove_nodes(GroupName::[group_name()], Nodes::[node()]) ->
                                ok | {error, Reason::term()}.
s_group_remove_nodes(GroupName, Nodes) ->
    request({s_group_remove_nodes, GroupName, Nodes}).

s_group_remove_nodes_check(GroupName, Nodes) ->
    request({s_group_remove_nodes_check, GroupName, Nodes}).


%% ==== ONLY for test suites ====
registered_names_test(Arg) ->
    request({registered_names_test, Arg}).
send_test(Name, Msg) ->
    request({send_test, Name, Msg}).
whereis_name_test(Name) ->
    request({whereis_name_test, Name}).
%% ==== ONLY for test suites ====


request(Req) ->
    request(Req, infinity).

request(Req, Time) ->
    case whereis(s_group) of
	P when is_pid(P) ->
	    gen_server:call(s_group, Req, Time);
	_Other -> 
	    {error, s_group_not_runnig}
    end.

%%%====================================================================================
%%% gen_server start
%%%
%%% The first thing to happen is to read if the s_groups key is defined in the
%%% .config file. If not defined, the whole system is started as one s_group, 
%%% and the services of s_group are superfluous.
%%% Otherwise a sync process is started to check that all nodes in the own global
%%% group have the same configuration. This is done by sending 'conf_check' to all
%%% other nodes and requiring 'conf_check_result' back.
%%% If the nodes are not in agreement of the configuration the s_group process 
%%% will remove these nodes from the #state.nodes list. This can be a normal case
%%% at release upgrade when all nodes are not yet upgraded.
%%%
%%% It is possible to manually force a sync of the s_group. This is done for 
%%% instance after a release upgrade, after all nodes in the group beeing upgraded.
%%% The nodes are not synced automatically because it would cause the node to be
%%% disconnected from those not yet beeing upgraded.
%%%
%%% The three process dictionary variables (registered_names, send, and whereis_name) 
%%% are used to store information needed if the search process crashes. 
%%% The search process is a help process to find registered names in the system.
%%%====================================================================================
start() -> gen_server:start({local, s_group}, s_group, [], []).
start_link() -> gen_server:start_link({local, s_group},s_group,[],[]).
stop() -> gen_server:call(s_group, stop, infinity).

init([]) ->
    process_flag(priority, max),
    ok = net_kernel:monitor_nodes(true),
    put(registered_names, [undefined]),
    put(send, [undefined]),
    put(whereis_name, [undefined]),
    process_flag(trap_exit, true),
    Ca = case init:get_argument(connect_all) of
	     {ok, [["false"]]} ->
		 false;
	     _ ->
		 true
	 end,
    PT = publish_arg(),
    case application:get_env(kernel, s_groups) of
	undefined ->
	    update_publish_nodes(PT),
	    {ok, #state{publish_type = PT,
			connect_all = Ca}};
	{ok, []} ->
	    update_publish_nodes(PT),
	    {ok, #state{publish_type = PT,
			connect_all = Ca}};
	{ok, NodeGrps} ->
            case catch config_scan(NodeGrps, publish_type) of
                {error, _Error2} ->
                    update_publish_nodes(PT),
                    exit({error, {'invalid g_groups definition', NodeGrps}});
                {ok, DefOwnSGroupsT, DefOtherSGroupsT} ->
                    ?debug({".config file scan result:",  {ok, DefOwnSGroupsT, DefOtherSGroupsT}}),
                    DefOwnSGroupsT1 = [{GroupName,GroupNodes}||
                                          {GroupName, _PubType, GroupNodes}
                                              <- DefOwnSGroupsT],
                    {DefSGroupNamesT1, DefSGroupNodesT1}=lists:unzip(DefOwnSGroupsT1),
                    DefSGroupNamesT = lists:usort(DefSGroupNamesT1),
                    DefSGroupNodesT = lists:usort(lists:append(DefSGroupNodesT1)),
                    update_publish_nodes(PT, {hidden, DefSGroupNodesT}),  
                    %% First disconnect any nodes not belonging to our own group
                    disconnect_nodes(nodes(connected) -- DefSGroupNodesT),
                    lists:foreach(fun(Node) ->
                                          erlang:monitor_node(Node, true)
                                  end,
                                  DefSGroupNodesT),
                    NewState = #state{publish_type = PT,
                                      sync_state = synced, 
                                      group_names = DefSGroupNamesT,
                                      no_contact = lists:delete(node(), DefSGroupNodesT),
                                      own_grps = DefOwnSGroupsT1,
                                      other_grps = DefOtherSGroupsT},
                    ?debug({"NewState", NewState}),
                    {ok, NewState}
            end
    end.
                        


%%%====================================================================================
%%% sync() -> ok 
%%%
%%% An operator ordered sync of the own global group. This must be done after
%%% a release upgrade. It can also be ordered if somthing has made the nodes
%%% to disagree of the s_groups definition.
%%%====================================================================================
handle_call(sync, _From, S) ->
    ?debug({"sync:",[node(), application:get_env(kernel, s_groups)]}),
    case application:get_env(kernel, s_groups) of
	undefined ->
	    update_publish_nodes(S#state.publish_type),
	    {reply, ok, S};
	{ok, []} ->
	    update_publish_nodes(S#state.publish_type),
	    {reply, ok, S};
	{ok, NodeGrps} ->
	    {DefGroupNames, PubTpGrp, DefNodes, DefOwn, DefOther} = 
		case catch config_scan(NodeGrps, publish_type) of
		    {error, _Error2} ->
			exit({error, {'invalid s_groups definition', NodeGrps}});
                    {ok, DefOwnSGroupsT, DefOtherSGroupsT} ->
                        ?debug({"sync:", {ok, DefOwnSGroupsT, DefOtherSGroupsT}}),
                        DefOwnSGroupsT1 = [{GroupName,GroupNodes}||
                                              {GroupName, _PubType, GroupNodes}
                                                  <- DefOwnSGroupsT],
                        {DefSGroupNamesT1, DefSGroupNodesT1}=lists:unzip(DefOwnSGroupsT1),
                        DefSGroupNamesT = lists:usort(DefSGroupNamesT1),
                        DefSGroupNodesT = lists:usort(lists:append(DefSGroupNodesT1)),
                        ?debug({"sync1:", DefSGroupNamesT, DefSGroupNodesT}),
                        update_publish_nodes(S#state.publish_type, {hidden, DefSGroupNodesT}),
                        %% First inform global on all nodes not belonging to our own group
                        NoGroupNodes =  nodes(connected) -- DefSGroupNodesT,
                        ?debug({"remove_from_no_group", node(), NoGroupNodes}),
                        [rpc:call(N, global, remove_from_no_group, [node()])
                         || N <-NoGroupNodes],
                        %%	disconnect_nodes(nodes(connected) -- DefSGroupNodesT), %%HL: is this needed?
			%% Sync with the nodes in the own group
                        kill_s_group_check(),
                        Pid = spawn_link(?MODULE, sync_init, 
					 [sync, DefSGroupNamesT, PubType, DefOwnSGroupsT1]),
			register(s_group_check, Pid),
                        {DefSGroupNamesT, PubType, 
                         lists:delete(node(), DefSGroupNodesT),
                         DefOwnSGroupsT1, DefOtherSGroupsT}
                end,
            {reply, ok, S#state{sync_state = synced, group_names = DefGroupNames, 
				no_contact = lists:sort(DefNodes), 
                                own_grps = DefOwn,
				other_grps = DefOther, group_publish_type = PubTpGrp}}
    end;



%%%====================================================================================
%%% Get the names of the s_groups
%%%====================================================================================
handle_call(s_groups, _From, S) ->
    Result = case S#state.sync_state of
		 no_conf ->
		     undefined;
		 synced ->
		     Other = lists:foldl(fun({N,_L}, Acc) -> Acc ++ [N]
					 end,
					 [], S#state.other_grps),
		     {S#state.group_names, Other}
	     end,
    {reply, Result, S};



%%%====================================================================================
%%% monitor_nodes(bool()) -> ok 
%%%
%%% Monitor nodes in the own global group. 
%%%   True => send nodeup/nodedown to the requesting Pid
%%%   False => stop sending nodeup/nodedown to the requesting Pid
%%%====================================================================================
handle_call({monitor_nodes, Flag}, {Pid, _}, StateIn) ->
    %%io:format("***** handle_call ~p~n",[monitor_nodes]),
    {Res, State} = monitor_nodes(Flag, Pid, StateIn),
    {reply, Res, State};


%%%====================================================================================
%%% own_nodes() -> [Node] 
%%%
%%% Get a list of nodes in the own global group
%%%====================================================================================
handle_call(own_nodes, _From, S) ->
    Nodes = case S#state.sync_state of
		no_conf ->
		    [node() | nodes()];
		synced ->
		    get_own_nodes()
                 %		    S#state.nodes
	    end,
    {reply, Nodes, S};



%%%====================================================================================
%%% registered_names({node, Node}) -> [Name] | {error, ErrorMessage}
%%% registered_names({group, GlobalGroupName}) -> [Name] | {error, ErrorMessage}
%%%
%%% Get the registered names from a specified Node, or GlobalGroupName.
%%%====================================================================================
handle_call({registered_names, {group, Group}}, From, S) ->
    case lists:member(Group, S#state.group_names) of 
        true ->
            Res = global:registered_names(),
            {reply, Res, S};
        false ->
            case lists:keysearch(Group, 1, S#state.other_grps) of
                false ->
                    {reply, [], S};
                {value, {Group, []}} ->
                    {reply, [], S};
                {value, {Group, Nodes}} ->
                    Pid = global_search:start(names, {group, Nodes, From}),
                    Wait = get(registered_names),
                    put(registered_names, [{Pid, From} | Wait]),
                    {noreply, S}
            end
    end;
handle_call({registered_names, {node, Node}}, _From, S) when Node =:= node() ->
    Res = global:registered_names(),
    {reply, Res, S};
handle_call({registered_names, {node, Node}}, From, S) ->
    Pid = global_search:start(names, {node, Node, From}),
    %%io:format(">>>>> registered_names Pid ~p~n",[Pid]),
    Wait = get(registered_names),
    put(registered_names, [{Pid, From} | Wait]),
    {noreply, S};



%%%====================================================================================
%%% send(Name, Msg) -> Pid | {badarg, {Name, Msg}}
%%% send({node, Node}, Name, Msg) -> Pid | {badarg, {Name, Msg}}
%%% send({group, GlobalGroupName}, Name, Msg) -> Pid | {badarg, {Name, Msg}}
%%%
%%% Send the Msg to the specified globally registered Name in own global group,
%%% in specified Node, or GlobalGroupName.
%%% But first the receiver is to be found, the thread is continued at
%%% handle_cast(send_res)
%%%====================================================================================
%% Search in the whole known world, but check own node first.
handle_call({send, Name, Msg}, From, S) ->
    case global:whereis_name(Name) of
	undefined ->
	    Pid = global_search:start(send, {any, S#state.other_grps, Name, Msg, From}),
	    Wait = get(send),
	    put(send, [{Pid, From, Name, Msg} | Wait]),
	    {noreply, S};
	Found ->
	    Found ! Msg,
	    {reply, Found, S}
    end;
%% Search in the specified global group, which happens to be the own group.
handle_call({send, {group, Grp}, Name, Msg}, From, S) ->
    case lists:member(Grp, S#state.group_names) of
        true ->
            case global:whereis_name(Name) of
                undefined ->
                    {reply, {badarg, {Name, Msg}}, S};
                Pid ->
                    Pid ! Msg,
                    {reply, Pid, S}
            end;
        false ->
            case lists:keysearch(Grp, 1, S#state.other_grps) of
                false ->
                    {reply, {badarg, {Name, Msg}}, S};
                {value, {Grp, []}} ->
                    {reply, {badarg, {Name, Msg}}, S};
                {value, {Grp, Nodes}} ->
                    Pid = global_search:start(send, {group, Nodes, Name, Msg, From}),
                    Wait = get(send),
                    put(send, [{Pid, From, Name, Msg} | Wait]),
                    {noreply, S}
            end
    end;
%% Search on the specified node.
handle_call({send, {node, Node}, Name, Msg}, From, S) ->
    Pid = global_search:start(send, {node, Node, Name, Msg, From}),
    Wait = get(send),
    put(send, [{Pid, From, Name, Msg} | Wait]),
    {noreply, S};



%%%====================================================================================
%%% whereis_name(Name) -> Pid | undefined
%%% whereis_name({node, Node}, Name) -> Pid | undefined
%%% whereis_name({group, GlobalGroupName}, Name) -> Pid | undefined
%%%
%%% Get the Pid of a globally registered Name in own global group,
%%% in specified Node, or GlobalGroupName.
%%% But first the process is to be found, 
%%% the thread is continued at handle_cast(find_name_res)
%%%====================================================================================
%% Search in the whole known world, but check own node first.
handle_call({whereis_name, Name}, From, S) ->
    case global:whereis_name(Name) of
	undefined ->
	    Pid = global_search:start(whereis, {any, S#state.other_grps, Name, From}),
	    Wait = get(whereis_name),
	    put(whereis_name, [{Pid, From} | Wait]),
	    {noreply, S};
	Found ->
	    {reply, Found, S}
    end;
%% Search in the specified global group, which happens to be the own group.
                                                % Need to change!! HL.
handle_call({whereis_name, {group, Group}, Name}, From, S) ->
    case lists:member(Group, S#state.group_names) of
        true ->
            Res = global:whereis_name(Name),
            {reply, Res, S};
        false ->
            case lists:keysearch(Group, 1, S#state.other_grps) of
                false ->
                    {reply, undefined, S};
                {value, {Group, []}} ->
                    {reply, undefined, S};
                {value, {Group, Nodes}} ->
                    Pid = global_search:start(whereis, {group, Nodes, Name, From}),
                    Wait = get(whereis_name),
                    put(whereis_name, [{Pid, From} | Wait]),
                    {noreply, S}
            end
    end;
%% Search on the specified node.
handle_call({whereis_name, {node, Node}, Name}, From, S) ->
    Pid = global_search:start(whereis, {node, Node, Name, From}),
    Wait = get(whereis_name),
    put(whereis_name, [{Pid, From} | Wait]),
    {noreply, S};


%%%====================================================================================
%%% s_groups parameter added to some other node which thinks that we
%%% belong to the same global group.
%%% It could happen that our node is not yet updated with the new node_group parameter
%%%====================================================================================
handle_call({ng_add_check, Node, PubType, OthersNG}, _From, S) ->
    %% Check which nodes are already updated
    OwnNG = get_own_nodes(),
    case S#state.group_publish_type =:= PubType of
	true ->
	    case OwnNG of
		OthersNG ->
		    NN = [Node | S#state.nodes],
		    NSE = lists:delete(Node, S#state.sync_error),
		    NNC = lists:delete(Node, S#state.no_contact),
		    NewS = S#state{nodes = lists:sort(NN), 
				   sync_error = NSE, 
				   no_contact = NNC},
		    {reply, agreed, NewS};
		_ ->
		    {reply, not_agreed, S}
	    end;
	_ ->
	    {reply, not_agreed, S}
    end;



%%%====================================================================================
%%% Misceleaneous help function to read some variables
%%%====================================================================================
handle_call(info, _From, S) ->    
    Reply = [{state,          S#state.sync_state},
	     {own_group_names, S#state.group_names},
             {own_group_nodes, lists:usort(lists:append(element(2,lists:unzip(S#state.own_grps))))},
           %%   {own_group_nodes, get_own_nodes()},
             %{"nodes()",      lists:sort(nodes())},
	     {synced_nodes,   S#state.nodes},
	     {sync_error,     S#state.sync_error},
	     {no_contact,     S#state.no_contact},
             {own_groups,     S#state.own_grps},
	     {other_groups,   S#state.other_grps},
             {publish_type,    S#state.publish_type},
             {group_publish_type, S#state.group_publish_type},
              {monitoring,     S#state.monitor}],

    {reply, Reply, S};

handle_call(get, _From, S) ->
    {reply, get(), S};

%%%====================================================================================
%%% Only for test suites. These tests when the search process exits.
%%%====================================================================================
handle_call({registered_names_test, {node, 'test3844zty'}}, From, S) ->
    Pid = global_search:start(names_test, {node, 'test3844zty'}),
    Wait = get(registered_names),
    put(registered_names, [{Pid, From} | Wait]),
    {noreply, S};
handle_call({registered_names_test, {node, _Node}}, _From, S) ->
    {reply, {error, illegal_function_call}, S};
handle_call({send_test, Name, 'test3844zty'}, From, S) ->
    Pid = global_search:start(send_test, 'test3844zty'),
    Wait = get(send),
    put(send, [{Pid, From, Name, 'test3844zty'} | Wait]),
    {noreply, S};
handle_call({send_test, _Name, _Msg }, _From, S) ->
    {reply, {error, illegal_function_call}, S};
handle_call({whereis_name_test, 'test3844zty'}, From, S) ->
    Pid = global_search:start(whereis_test, 'test3844zty'),
    Wait = get(whereis_name),
    put(whereis_name, [{Pid, From} | Wait]),
    {noreply, S};
handle_call({whereis_name_test, _Name}, _From, S) ->
    {reply, {error, illegal_function_call}, S};



%%%====================================================================================
%%% Create a new s_group.
%%% -spec new_s_group(GroupName::group_name(), Nodes::[node()]) ->
%%%                         ok|{error, Reason::term()}.
%%%====================================================================================
handle_call({new_s_group, GroupName, Nodes}, _From, S) ->
    ?debug({new_s_group, GroupName, Nodes}),
    OtherNodes = lists:delete(node(), Nodes),
    OwnKnownSgroupNames = S#state.group_names, 
    OthersKnownSgroupNames = known_s_group_names(OtherNodes),
    KnownSgroupNames=OwnKnownSgroupNames ++  OthersKnownSgroupNames, 
    ?debug({"KnownSGroupNames:",KnownSgroupNames}), 
    case lists:member(GroupName, KnownSgroupNames) of 
        true ->
            {reply, {error, s_group_name_in_use}, S};
        false ->
            {reply, ok, S}
    end;

handle_call({new_s_group_1, GroupName, Nodes}, _From, S) ->
    ?debug({new_s_group, GroupName, Nodes}),
    OtherNodes = lists:delete(node(), Nodes),
    ?debug({conntect, nodes(connected)}),
    NewConf=mk_new_s_group_conf(GroupName, Nodes),
    application:set_env(kernel, s_groups, NewConf),
    NGACArgs = [node(), hidden, {GroupName, Nodes}],
    {_NS, _NNC, _NSE} =
        lists:foldl(fun(Node, {NN_acc, NNC_acc, NSE_acc}) -> 
                            case rpc:call(Node, s_group, new_s_group_check, NGACArgs) of
                                {badrpc, _} ->
                                    {NN_acc, [Node | NNC_acc], NSE_acc};
                                agreed ->
                                            {[Node | NN_acc], NNC_acc, NSE_acc};
                                not_agreed ->
                                    {NN_acc, NNC_acc, [Node | NSE_acc]}
                            end
                    end,
                    {[], [], []}, OtherNodes),
    NewS= S#state{sync_state = synced, 
                  group_names = [GroupName|S#state.group_names], 
                  sync_error = Nodes ++ S#state.sync_error, 
                  no_contact= Nodes ++ S#state.no_contact,
                  own_grps = [{GroupName,Nodes}|S#state.own_grps]},
    {reply, ok, NewS};

handle_call({new_s_group_check, Node, PubType, {GroupName, Nodes}}, _From, S) ->
    ?debug({{new_s_group_check, Node, PubType, {GroupName, Nodes}}, _From, S}),
    NewConf=mk_new_s_group_conf(GroupName, Nodes),
    application:set_env(kernel, s_groups, NewConf),
    PT = publish_arg(),
    update_publish_nodes(PT, {PubType, Nodes}),  .
    OtherNodes = Nodes -- [node()],
    GroupNodes = Nodes++lists:append(element(2,lists:unzip(S#state.own_grps))),
    NoGroupNodes =  nodes(connected) -- GroupNodes,
    ?debug({"remove_from_no_group", node(), NoGroupNodes}),
    [rpc:call(N, global, remove_from_no_group, [node()])
     || N <-NoGroupNodes],
    NewS= S#state{sync_state = synced, 
                  group_names = [GroupName|S#state.group_names], 
                  sync_error = OtherNodes ++ S#state.sync_error, 
                  no_contact= OtherNodes  ++ S#state.no_contact,
                  own_grps = [{GroupName,Nodes}|S#state.own_grps]},
    {reply, agreed, NewS};

%%%====================================================================================
%%% Remove a s_group.
%%% -spec remove_s_group(GroupName::group_name()) -> ok | {error, Reason::term()}.
%%%====================================================================================
handle_call({remove_s_group, GroupName}, _From, S) ->
    ?debug({remove_s_group, GroupName}),
    case lists:member(GroupName, S#state.group_names) of 
        false ->
            {reply, ok, S};
        true ->
            global:remove_s_group(GroupName),
            NewConf=rm_s_group_from_conf(GroupName),
            application:set_env(kernel, s_groups, NewConf),
            GroupNodes = case lists:keyfind(GroupName, 1, S#state.own_grps) of 
                             {_, Ns}-> Ns;
                             false -> []  %% this should not happen.
                         end,
            OtherGroupNodes = lists:usort(lists:append(
                                            [Ns||{G, Ns}<-S#state.own_grps,G/=GroupName])),
            OtherNodes = lists:delete(node(), GroupNodes),
            [rpc:call(Node, s_group, remove_s_group_check, [GroupName])||
                Node<-OtherNodes],
            NewS = S#state{sync_state = if length(S#state.group_names)==1 -> no_conf;
                                           true -> synced
                                        end,
                           group_names = S#state.group_names -- [GroupName],
                           nodes = S#state.nodes -- (GroupNodes--OtherGroupNodes),
                           sync_error = S#state.sync_error--GroupNodes, 
                           no_contact = S#state.no_contact--GroupNodes,
                           own_grps = S#state.own_grps -- [{GroupName, GroupNodes}] 
                          },
            {reply, ok, NewS}
    end;

handle_call({remove_s_group_check, GroupName}, _From, S) ->
    ?debug({remove_s_group_check, GroupName}),
    global:remove_s_group(GroupName),
    NewConf=rm_s_group_from_conf(GroupName),
    application:set_env(kernel, s_groups, NewConf),
    GroupNodes = case lists:keyfind(GroupName, 1, S#state.own_grps) of 
                     {_, Ns}-> Ns;
                     false -> []  %% this should not happen.
                 end,
    OtherGroupNodes = lists:usort(lists:append(
                                    [Ns||{G, Ns}<-S#state.own_grps,G/=GroupName])),
    NewS = S#state{sync_state = if length(S#state.group_names)==1 -> no_conf;
                                   true -> synced
                                end, 
                   group_names = S#state.group_names -- [GroupName],
                   nodes = S#state.nodes -- (GroupNodes--OtherGroupNodes),
                   sync_error = S#state.sync_error--GroupNodes, 
                   no_contact = S#state.no_contact--GroupNodes,
                   own_grps = S#state.own_grps -- [{GroupName, GroupNodes}] 
                  },
    
    {reply, ok, NewS};



%%%====================================================================================
%%% %%% -spec s_group_remove_nodes(GroupName::[group_name()], Nodes::[node()]) ->
%%%                                ok | {error, Reason::term()}.
%%%====================================================================================
handle_call({s_group_remove_nodes, GroupName, Nodes}, _From, S) ->
    ?debug({s_group_remove_nodes, GroupName, Nodes}),
    case lists:member(GroupName, S#state.group_names) of 
        false ->
            {reply, {error, s_group_name_does_not_exist}, S};
        true ->
            GroupNodes = case lists:keyfind(GroupName, 1, S#state.own_grps) of 
                             {_, Ns}-> Ns;
                             false -> []  %% this should not happen.
                         end, 
            OtherNodes = GroupNodes -- [node()],
            NodesToRm = intersection(Nodes, GroupNodes),
            NewConf = rm_s_group_nodes_from_conf(GroupName,NodesToRm),
            application:set_env(kernel, s_groups, NewConf),
            global:remove_s_group_nodes(GroupName, NodesToRm),
            [rpc:call(Node, s_group, s_group_remove_nodes_check, [GroupName, NodesToRm])||
                Node<-OtherNodes],
            NewS=case lists:member(node(), NodesToRm) orelse NodesToRm==GroupNodes of 
                     true ->
                         NewOwnGrps = [Ns||{G, Ns}<-S#state.own_grps, G/=GroupName],
                         NewNodes = lists:append(element(2,lists:unzip(NewOwnGrps))),
                         S#state{sync_state = if length(S#state.group_names)==1 -> no_conf;
                                                 true -> synced
                                              end, 
                                 group_names = S#state.group_names -- [GroupName],
                                 nodes = NewNodes,
                                 sync_error = S#state.sync_error--GroupNodes, 
                                 no_contact = S#state.no_contact--GroupNodes,
                                 own_grps = NewOwnGrps 
                                };
                     false-> 
                         NewGroupNodes = GroupNodes -- NodesToRm,
                         NewOwnGrps = lists:keyreplace(GroupName,1, S#state.own_grps,
                                                       {GroupName, NewGroupNodes}),
                         NewNodes = lists:append(element(2,lists:unzip(NewOwnGrps))),
                         S#state{nodes = NewNodes,
                                 sync_error = S#state.sync_error--NodesToRm, 
                                 no_contact = S#state.no_contact--NodesToRm,
                                 own_grps = NewOwnGrps}
                 end,
            {reply, ok, NewS}
    end;
     
handle_call({s_group_remove_nodes_check, GroupName, NodesToRm}, _From, S) ->
    ?debug({{s_group_remove_nodes_check, GroupName, NodesToRm}, _From, S}),
    NewConf = rm_s_group_nodes_from_conf(GroupName,NodesToRm),
    application:set_env(kernel, s_groups, NewConf),
    global:remove_s_group_nodes(GroupName, NodesToRm),
    GroupNodes = case lists:keyfind(GroupName, 1, S#state.own_grps) of 
                             {_, Ns}-> Ns;
                             false -> []  %% this should not happen.
                         end,
    NewS=case lists:member(node(), NodesToRm) orelse NodesToRm==GroupNodes of 
                     true ->
                         NewOwnGrps = [Ns||{G, Ns}<-S#state.own_grps, G/=GroupName],
                         NewNodes = lists:append(element(2,lists:unzip(NewOwnGrps))),
                         S#state{sync_state = if length(S#state.group_names)==1 -> no_conf;
                                                 true -> synced
                                              end, 
                                 group_names = S#state.group_names -- [GroupName],
                                 nodes = NewNodes,
                                 sync_error = S#state.sync_error--GroupNodes, 
                                 no_contact = S#state.no_contact--GroupNodes,
                                 own_grps = NewOwnGrps 
                                };
                     false-> 
                         NewGroupNodes = GroupNodes -- NodesToRm,
                         NewOwnGrps = lists:keyreplace(GroupName,1, S#state.own_grps,
                                                       {GroupName, NewGroupNodes}),
                         NewNodes = lists:append(element(2,lists:unzip(NewOwnGrps))),
                         S#state{nodes = NewNodes,
                                 sync_error = S#state.sync_error--NodesToRm, 
                                 no_contact = S#state.no_contact--NodesToRm,
                                 own_grps = NewOwnGrps}
                 end,
    {reply, ok, NewS};
  

%%%====================================================================================
%%% remove nodes from an existing s_group.
%%%-spec s_group_add_nodes(GroupName::group_name(), Nodes::[node()]) ->
%%%                               ok | {error, Reason::term()}.
%%%====================================================================================
handle_call({s_group_add_nodes, GroupName, Nodes}, _From, S) ->
    ?debug({s_group_add_nodes, GroupName, Nodes}),
    case lists:member(GroupName, S#state.group_names) of 
        false ->
            {reply, {error, s_group_name_does_not_exist}, S};
        true ->
            GroupNodes = case lists:keyfind(GroupName, 1, S#state.own_grps) of 
                             {_, Ns}-> Ns;
                             false -> []  %% this should not happen.
                         end,
            case Nodes -- GroupNodes of 
                [] -> {reply, ok, S};
                NewNodes ->
                    ok=global:set_s_group_name(GroupName),
                    NodesToDisConnect =nodes(connected) -- (Nodes++S#state.nodes),
                    force_nodedown(NodesToDisConnect),
                    NewGroupNodes=lists:sort(NewNodes++GroupNodes),
                    NewConf=mk_new_s_group_conf(GroupName, NewGroupNodes),
                    ?debug({"Newconf:", NewConf}),
                    application:set_env(kernel, s_groups, NewConf),
                    NewGroupNodes=lists:sort(NewNodes++GroupNodes),
                    NGACArgs = [node(), normal, {GroupName, NewGroupNodes}],
                    ?debug({"NGACArgs:",NGACArgs}),
                    {NS, NNC, NSE} =
                        lists:foldl(fun(Node, {NN_acc, NNC_acc, NSE_acc}) -> 
                                            case rpc:call(Node, s_group, s_group_add_nodes_check, NGACArgs) of
                                                {badrpc, _} ->
                                                    {NN_acc, [Node | NNC_acc], NSE_acc};
                                                agreed ->
                                                    {[Node | NN_acc], NNC_acc, NSE_acc};
                                                not_agreed ->
                                                    {NN_acc, NNC_acc, [Node | NSE_acc]}
                                            end
                                    end,
                                    {[], [], []}, lists:delete(node(), NewGroupNodes)),
                    ?debug({"NS_NNC_NSE1:",  {NS, NNC, NSE}}),
                    ok = global:reset_s_group_name(),
                    NewS = S#state{sync_state = synced, 
                                   nodes = lists:usort(NewNodes++S#state.nodes),
                                   sync_error = lists:usort(NSE++S#state.sync_error--NS), 
                                   no_contact = lists:usort(NNC++S#state.no_contact--NS),
                                   own_grps =  lists:keyreplace(GroupName,1, S#state.own_grps,
                                                                {GroupName, NewGroupNodes})                                
                                  },
                    {reply, ok, NewS}
            end
    end;

handle_call({s_group_add_nodes_check, Node, _PubType, {GroupName, Nodes}}, _From, S) ->
    ?debug({{s_group_add_nodes_check, Node, _PubType, {GroupName, Nodes}}, _From, S}),
    NewConf=mk_new_s_group_conf(GroupName, Nodes),
    application:set_env(kernel, s_groups, NewConf),
    NewGroupNames = case lists:member(GroupName, S#state.group_names) of
                        true->S#state.group_names;
                        false-> lists:sort([GroupName|S#state.group_names])
                    end,
    NewOwnGroups = case lists:keyfind(GroupName, 1, S#state.own_grps) of
                        false -> [{GroupName, Nodes}];
                        _-> lists:keyreplace(GroupName, 1,
                                             S#state.own_grps,
                                             {GroupName, Nodes})
                    end,
    NewS= S#state{sync_state = synced,
                  group_names = NewGroupNames,
                  nodes = lists:usort(S#state.nodes++Nodes),
                  sync_error = lists:delete(Node, S#state.sync_error),
                  no_contact=lists:delete(Node, S#state.no_contact),
                  own_grps = NewOwnGroups},
    {reply, agreed, NewS};
          
%%%====================================================================================
handle_call(Call, _From, S) ->
     %%io:format("***** handle_call ~p~n",[Call]),
    {reply, {illegal_message, Call}, S}.

%%%====================================================================================
%%% registered_names({node, Node}) -> [Name] | {error, ErrorMessage}
%%% registered_names({group, GlobalGroupName}) -> [Name] | {error, ErrorMessage}
%%%
%%% Get a list of nodes in the own global group
%%%====================================================================================
handle_cast({registered_names, User}, S) ->
    %%%io:format(">>>>> registered_names User ~p~n",[User]),
    Res = global:registered_names(),
    User ! {registered_names_res, Res},
    {noreply, S};

handle_cast({registered_names_res, Result, Pid, From}, S) ->
    %%%io:format(">>>>> registered_names_res Result ~p~n",[Result]),
    unlink(Pid),
    exit(Pid, normal),
    Wait = get(registered_names),
    NewWait = lists:delete({Pid, From},Wait),
    put(registered_names, NewWait),
    gen_server:reply(From, Result),
    {noreply, S};



%%%====================================================================================
%%% send(Name, Msg) -> Pid | {error, ErrorMessage}
%%% send({node, Node}, Name, Msg) -> Pid | {error, ErrorMessage}
%%% send({group, GlobalGroupName}, Name, Msg) -> Pid | {error, ErrorMessage}
%%%
%%% The registered Name is found; send the message to it, kill the search process,
%%% and return to the requesting process.
%%%====================================================================================
handle_cast({send_res, Result, Name, Msg, Pid, From}, S) ->
    %%%io:format("~p>>>>> send_res Result ~p~n",[node(), Result]),
    case Result of
	{badarg,{Name, Msg}} ->
	    continue;
	ToPid ->
	    ToPid ! Msg
    end,
    unlink(Pid),
    exit(Pid, normal),
    Wait = get(send),
    NewWait = lists:delete({Pid, From, Name, Msg},Wait),
    put(send, NewWait),
    gen_server:reply(From, Result),
    {noreply, S};



%%%====================================================================================
%%% A request from a search process to check if this Name is registered at this node.
%%%====================================================================================
handle_cast({find_name, User, Name}, S) ->
    Res = global:whereis_name(Name),
    %%%io:format(">>>>> find_name Name ~p   Res ~p~n",[Name, Res]),
    User ! {find_name_res, Res},
    {noreply, S};

%%%====================================================================================
%%% whereis_name(Name) -> Pid | undefined
%%% whereis_name({node, Node}, Name) -> Pid | undefined
%%% whereis_name({group, GlobalGroupName}, Name) -> Pid | undefined
%%%
%%% The registered Name is found; kill the search process
%%% and return to the requesting process.
%%%====================================================================================
handle_cast({find_name_res, Result, Pid, From}, S) ->
    %%%io:format(">>>>> find_name_res Result ~p~n",[Result]),
    %%%io:format(">>>>> find_name_res get() ~p~n",[get()]),
    unlink(Pid),
    exit(Pid, normal),
    Wait = get(whereis_name),
    NewWait = lists:delete({Pid, From},Wait),
    put(whereis_name, NewWait),
    gen_server:reply(From, Result),
    {noreply, S};


%%%====================================================================================
%%% The node is synced successfully
%%%====================================================================================
handle_cast({synced, NoContact}, S) ->
    %io:format("~p>>>>> synced ~p  ~n",[node(), NoContact]),
    kill_s_group_check(),
    Nodes = get_own_nodes() -- [node() | NoContact],
    {noreply, S#state{nodes = lists:sort(Nodes),
		      sync_error = [],
		      no_contact = NoContact}};    


%%%====================================================================================
%%% The node could not sync with some other nodes.
%%%====================================================================================
handle_cast({sync_error, NoContact, ErrorNodes}, S) ->
    Txt = io_lib:format("Global group: Could not synchronize with these nodes ~p~n"
			"because s_groups were not in agreement. ~n", [ErrorNodes]),
    error_logger:error_report(Txt),
    ?debug(lists:flatten(Txt)),
    kill_s_group_check(),
    Nodes = (get_own_nodes() -- [node() | NoContact]) -- ErrorNodes,
    {noreply, S#state{nodes = lists:sort(Nodes), 
		      sync_error = ErrorNodes,
		      no_contact = NoContact}};


%%%====================================================================================
%%% Another node is checking this node's group configuration
%%%====================================================================================
handle_cast({conf_check, Vsn, Node, From, sync, CCName, CCNodes}, S) ->
    handle_cast({conf_check, Vsn, Node, From, sync, CCName, normal, CCNodes}, S);

handle_cast({conf_check, Vsn, Node, From, sync, CCName, PubType, CCNodes}, S) ->
    CurNodes = S#state.nodes,
    %    io:format(">>>>> conf_check,sync  Node ~p~n",[Node]),
    %% Another node is syncing, 
    %% done for instance after upgrade of global_groups parameter
    NS = 
	case application:get_env(kernel, s_groups) of
	    undefined ->
		%% We didn't have any node_group definition
		update_publish_nodes(S#state.publish_type),
		disconnect_nodes([Node]),
		{s_group_check, Node} ! {config_error, Vsn, From, node()},
		S;
	    {ok, []} ->
		%% Our node_group definition was empty
		update_publish_nodes(S#state.publish_type),
		disconnect_nodes([Node]),
		{s_group_check, Node} ! {config_error, Vsn, From, node()},
		S;
	    %%---------------------------------
	    %% s_groups defined
	    %%---------------------------------
	    {ok, NodeGrps} ->
                case config_scan(NodeGrps, publish_type) of
		    {error, _Error2} ->
			%% Our node_group definition was erroneous
			disconnect_nodes([Node]),
			{s_group_check, Node} ! {config_error, Vsn, From, node()},
			S#state{nodes = lists:delete(Node, CurNodes)};
                    {ok, OwnSGroups, _OtherSGroups} ->
                        case lists:keyfind(CCName, 1, OwnSGroups) of
                            {CCName, PubType, CCNodes} ->
                                %% OK, add the node to the #state.nodes if it isn't there
                                update_publish_nodes(S#state.publish_type, {PubType, CCNodes}),
                                ?debug({global_name_server, {nodeup, CCName,Node}}),
                                global_name_server ! {nodeup, CCName, Node},
                                {s_group_check, Node} ! {config_ok, Vsn, From, CCName, node()},
                                case lists:member(Node, CurNodes) of
                                    false ->
                                        NewNodes = lists:sort([Node | CurNodes]),
                                        NSE = lists:delete(Node, S#state.sync_error),
                                        NNC = lists:delete(Node, S#state.no_contact),
                                        S#state{nodes = NewNodes, 
                                                sync_error = NSE,
                                                no_contact = NNC};
                                    true ->
                                        S
                                end;
                            _ ->
                                %% node_group definitions were not in agreement
                                disconnect_nodes([Node]),
                                {s_group_check, Node} ! {config_error, Vsn, From, node()},
                                NN = lists:delete(Node, S#state.nodes),
                                NSE = lists:delete(Node, S#state.sync_error),
                                NNC = lists:delete(Node, S#state.no_contact),
                                S#state{nodes = NN,
                                        sync_error = NSE,
                                        no_contact = NNC}
                        end
                end
        end,
    {noreply, NS};

handle_cast(_Cast, S) ->
%    io:format("***** handle_cast ~p~n",[_Cast]),
    {noreply, S}.
    


%%%====================================================================================
%%% A node went down. If no global group configuration inform global;
%%% if global group configuration inform global only if the node is one in
%%% the own global group.
%%%====================================================================================
handle_info({nodeup, Node}, S) when S#state.sync_state =:= no_conf ->
    %% case application:get_env(kernel, s_groups) of 
    %%     undefined ->
            ?debug({"NodeUp:",  node(), Node}),
            send_monitor(S#state.monitor, {nodeup, Node}, S#state.sync_state),
            GroupName = rpc:call(Node, global, get_s_group_name, []), 
            global_name_server ! {nodeup, GroupName, Node},
            {noreply, S};
    %%     _ ->
    %%         handle_node_up(Node,S)
    %% end;      

handle_info({nodeup, Node}, S) ->  %% Need to test!!!
    ?debug({"NodeUp:",  node(), Node}),
    case  rpc:call(Node, global, get_s_group_name, []) of 
        no_group ->
            ?debug({"s_group_name:", no_group}),
            handle_node_up(Node, S);
        GroupName ->
            ?debug({"s_group_name:", GroupName}),
            send_monitor(S#state.monitor, {nodeup, Node}, S#state.sync_state),
            GroupName = rpc:call(Node, global, get_s_group_name, []), 
            global_name_server ! {nodeup, GroupName, Node},
            {noreply, S}
    end;
%%%====================================================================================
%%% A node has crashed. 
%%% nodedown must always be sent to global; this is a security measurement
%%% because during release upgrade the s_groups parameter is upgraded
%%% before the node is synced. This means that nodedown may arrive from a
%%% node which we are not aware of.
%%%====================================================================================
handle_info({nodedown, Node}, S) when S#state.sync_state =:= no_conf ->
%    io:format("~p>>>>> nodedown, no_conf Node ~p~n",[node(), Node]),
    send_monitor(S#state.monitor, {nodedown, Node}, S#state.sync_state),
    global_name_server ! {nodedown, Node},
    {noreply, S};
handle_info({nodedown, Node}, S) ->
%    io:format("~p>>>>> nodedown, Node ~p  ~n",[node(), Node]),
    send_monitor(S#state.monitor, {nodedown, Node}, S#state.sync_state),
    global_name_server ! {nodedown, Node},
    NN = lists:delete(Node, S#state.nodes),
    NSE = lists:delete(Node, S#state.sync_error),
    NNC = case {lists:member(Node, get_own_nodes()), 
		lists:member(Node, S#state.no_contact)} of
	      {true, false} ->
		  [Node | S#state.no_contact];
	      _ ->
		  S#state.no_contact
	  end,
    {noreply, S#state{nodes = NN, no_contact = NNC, sync_error = NSE}};


%%%====================================================================================
%%% A node has changed its s_groups definition, and is telling us that we are not
%%% included in his group any more. This could happen at release upgrade.
%%%====================================================================================
handle_info({disconnect_node, Node}, S) ->
%    io:format("~p>>>>> disconnect_node Node ~p CN ~p~n",[node(), Node, S#state.nodes]),
    case {S#state.sync_state, lists:member(Node, S#state.nodes)} of
	{synced, true} ->
	    send_monitor(S#state.monitor, {nodedown, Node}, S#state.sync_state);
	_ ->
	    cont
    end,
    global_name_server ! {nodedown, Node}, %% nodedown is used to inform global of the
                                           %% disconnected node
    NN = lists:delete(Node, S#state.nodes),
    NNC = lists:delete(Node, S#state.no_contact),
    NSE = lists:delete(Node, S#state.sync_error),
    {noreply, S#state{nodes = NN, no_contact = NNC, sync_error = NSE}};




handle_info({'EXIT', ExitPid, Reason}, S) ->
    check_exit(ExitPid, Reason),
    {noreply, S};


handle_info(_Info, S) ->
%    io:format("***** handle_info = ~p~n",[_Info]),
    {noreply, S}.

handle_node_up(Node, S) ->
    OthersNG = case (catch rpc:call(Node, s_group, get_own_s_groups_with_nodes, [])) of
                   X when is_list(X) ->
                       X;
                   _ ->
                       []
               end,
    ?debug({"OthersNG:",OthersNG}),
    OwnNGs = get_own_s_groups_with_nodes(),
    OwnGroups = element(1, lists:unzip(OwnNGs)),
    ?debug({"ownsNG:",OwnNGs}),
    NNC = lists:delete(Node, S#state.no_contact),
    NSE = lists:delete(Node, S#state.sync_error),
    case shared_s_groups_match(OwnNGs, OthersNG) of 
        true->
            ?debug("Same s_ group match.\n"),
            %% OwnGroups =  S#state.group_names,
            OthersGroups = element(1, lists:unzip(OthersNG)),
            CommonGroups = intersection(OwnGroups, OthersGroups),
            send_monitor(S#state.monitor, {nodeup, Node}, S#state.sync_state),
            ?debug({nodeup, OwnGroups, Node, CommonGroups}),
	    [global_name_server ! {nodeup, Group, Node}||Group<-CommonGroups],  
	    case lists:member(Node, S#state.nodes) of
		false ->
		    NN = lists:usort([Node | S#state.nodes]),
		    {noreply, S#state{
                                sync_state=synced,
                                group_names = OwnGroups,
                                nodes = NN, 
                                no_contact = NNC,
                                sync_error = NSE}};
		true ->
		    {noreply, S#state{
                                sync_state=synced,
                                group_names = OwnGroups,
                                no_contact = NNC,
                                sync_error = NSE}}
	    end;
	false ->
            ?debug("Same s_ group NOT match.\n"),
            case {lists:member(Node, get_own_nodes()), 
	          lists:member(Node, S#state.sync_error)} of
	        {true, false} ->
	            NSE2 = lists:usort([Node | S#state.sync_error]),
	            {noreply, S#state{
                                sync_state = synced,
                                group_names = OwnGroups,
                                no_contact = NNC,
                                sync_error = NSE2}};
                _ ->
                    {noreply, S#state{sync_state=synced,
                                      group_names = OwnGroups}}
	    end
    end.



shared_s_groups_match(OwnSGroups, OthersSGroups) ->
    OwnSGroupNames = [G||{G, _Nodes}<-OwnSGroups],
    OthersSGroupNames = [G||{G, _Nodes}<-OthersSGroups],
    SharedSGroups = intersection(OwnSGroupNames, OthersSGroupNames),
    case SharedSGroups of 
        [] -> false;
        Gs ->
            Own =[{G, lists:sort(Nodes)}
                  ||{G, Nodes}<-OwnSGroups, lists:member(G, Gs)],
            Others= [{G, lists:sort(Nodes)}
                     ||{G, Nodes}<-OthersSGroups, lists:member(G, Gs)],
            lists:sort(Own) == lists:sort(Others)
    end.
intersection(_, []) -> 
    [];
intersection(L1, L2) ->
    L1 -- (L1 -- L2).


terminate(_Reason, _S) ->
    ok.
    

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%====================================================================================
%%% Check the global group configuration.
%%%====================================================================================

%% type spec added by HL.
-spec config_scan(NodeGrps::[group_tuple()])->
                         {ok, OwnGrps::[{group_name(), [node()]}], 
                          OtherGrps::[{group_name(), [node()]}]}
                             |{error, any()}.

%% Functionality rewritten by HL.
config_scan(NodeGrps) ->
    config_scan(NodeGrps, original).

config_scan(NodeGrps, original) ->
     config_scan(NodeGrps, publish_type);

config_scan(NodeGrps, publish_type) ->
    config_scan(node(), NodeGrps, [], []).

config_scan(_MyNode, [], MyOwnNodeGrps, OtherNodeGrps) ->
    {ok, MyOwnNodeGrps, OtherNodeGrps};
config_scan(MyNode, [GrpTuple|NodeGrps], MyOwnNodeGrps, OtherNodeGrps) ->
    {GrpName, PubTypeGroup, Nodes} = grp_tuple(GrpTuple),
    case lists:member(MyNode, Nodes) of
	true ->
            %% HL: is PubTypeGroup needed?
            config_scan(MyNode, NodeGrps, 
                        [{GrpName, PubTypeGroup,lists:sort(Nodes)}
                         |MyOwnNodeGrps], 
                        OtherNodeGrps);
	false ->
	    config_scan(MyNode,NodeGrps, MyOwnNodeGrps,
                        [{GrpName, lists:sort(Nodes)}|
                         OtherNodeGrps])
    end.

grp_tuple({Name, Nodes}) ->
    {Name, hidden, Nodes};   %% for s_groups, hidden is the default!!!
grp_tuple({Name, hidden, Nodes}) ->
    {Name, hidden, Nodes};
grp_tuple({Name, normal, Nodes}) ->
    {Name, normal, Nodes}.

 
%%%====================================================================================
%%% The special process which checks that all nodes in the own global group
%%% agrees on the configuration.
%%%====================================================================================
-spec sync_init(_, _, _, _) -> no_return().
sync_init(Type, _Cname, PubType, SGroupNodesPairs) ->
    ?debug({"Sync int:", Type, _Cname, PubType, SGroupNodesPairs}),
    NodeGroupPairs = lists:append([[{Node, GroupName}||Node<-Nodes]
                                   ||{GroupName, Nodes}<-SGroupNodesPairs]),
    Nodes = lists:usort(element(1,lists:unzip(NodeGroupPairs))),
    ?debug({"node(), Nodes:", node(), Nodes}),
    {Up, Down} = sync_check_node(lists:delete(node(), Nodes), [], []),
    ?debug({"updown:", Up, Down}),
    sync_check_init(Type, Up, NodeGroupPairs, Down, PubType).

sync_check_node([], Up, Down) ->
    {Up, Down};
sync_check_node([Node|Nodes], Up, Down) ->
    case net_adm:ping(Node) of
	pang ->
	    sync_check_node(Nodes, Up, [Node|Down]);
	pong ->
	    sync_check_node(Nodes, [Node|Up], Down)
    end.



%%%-------------------------------------------------------------
%%% Check that all nodes are in agreement of the global
%%% group configuration.
%%%-------------------------------------------------------------
-spec sync_check_init(_, _, _, _, _) -> no_return().
sync_check_init(Type, Up, NodeGroupPairs, Down, PubType) ->
    sync_check_init(Type, Up, NodeGroupPairs, 3, [], Down, PubType).

-spec sync_check_init(_, _, _, _, _, _,  _) -> no_return().
sync_check_init(_Type, NoContact, _NodeGroupPairss, 0,
                ErrorNodes, Down, _PubType) ->
    case ErrorNodes of
	[] -> 
	    gen_server:cast(s_group, {synced, lists:sort(NoContact ++ Down)});
	_ ->
	    gen_server:cast(s_group, {sync_error, lists:sort(NoContact ++ Down),
					   ErrorNodes})
    end,
    receive
	kill ->
	    exit(normal)
    after 5000 ->
	    exit(normal)
    end;

sync_check_init(Type, Up, NodeGroupPairs, N, ErrorNodes, Down, PubType) ->
    lists:foreach(fun(Node) ->
                          {Node, Group} = lists:keyfind(Node, 1, NodeGroupPairs),
                          GroupNodes = [Node1||{Node1, G}<-NodeGroupPairs, G==Group],
                          ConfCheckMsg = 
                              case PubType of
                                  normal ->
                                      {conf_check, ?cc_vsn, node(), self(), Type, 
                                       Group, GroupNodes};
                                  _ ->
                                      {conf_check, ?cc_vsn, node(), self(), Type,
                                       Group, PubType, GroupNodes}
                              end,
                          ?debug({conf_check, s_group, Node, ConfCheckMsg}),
                          gen_server:cast({s_group, Node}, ConfCheckMsg)
		  end, Up),
    case sync_check(Up) of
	{ok, synced} ->
	    sync_check_init(Type, [],NodeGroupPairs, 0,
                            ErrorNodes, Down, PubType);
	{error, NewErrorNodes} ->
	    sync_check_init(Type, [], NodeGroupPairs, 0,
                            ErrorNodes ++ NewErrorNodes, Down, PubType);
	{more, Rem, NewErrorNodes} ->
	    %% Try again to reach the s_group, 
	    %% obviously the node is up but not the s_group process.
	    sync_check_init(Type, Rem, NodeGroupPairs, N - 1,
                            ErrorNodes ++ NewErrorNodes, Down, PubType)
    end.

sync_check(Up) ->
    sync_check(Up, Up, []).

sync_check([], _Up, []) ->
    {ok, synced};
sync_check([], _Up, ErrorNodes) ->
    {error, ErrorNodes};
sync_check(Rem, Up, ErrorNodes) ->
    receive
	{config_ok, ?cc_vsn, Pid, GroupName, Node} when Pid =:= self() ->
	    global_name_server ! {nodeup, GroupName, Node},
	    sync_check(Rem -- [Node], Up, ErrorNodes);
	{config_error, ?cc_vsn, Pid, Node} when Pid =:= self() ->
	    sync_check(Rem -- [Node], Up, [Node | ErrorNodes]);
	{no_s_group_configuration, ?cc_vsn, Pid, Node} when Pid =:= self() ->
	    sync_check(Rem -- [Node], Up, [Node | ErrorNodes]);
	%% Ignore, illegal vsn or illegal Pid
	_ ->
	    sync_check(Rem, Up, ErrorNodes)
    after 2000 ->
	    %% Try again, the previous conf_check message  
	    %% apparently disapared in the magic black hole.
	    {more, Rem, ErrorNodes}
    end.


%%%====================================================================================
%%% A process wants to toggle monitoring nodeup/nodedown from nodes.
%%%====================================================================================
monitor_nodes(true, Pid, State) ->
    link(Pid),
    Monitor = State#state.monitor,
    {ok, State#state{monitor = [Pid|Monitor]}};
monitor_nodes(false, Pid, State) ->
    Monitor = State#state.monitor,
    State1 = State#state{monitor = delete_all(Pid,Monitor)},
    do_unlink(Pid, State1),
    {ok, State1};
monitor_nodes(_, _, State) ->
    {error, State}.

delete_all(From, [From |Tail]) -> delete_all(From, Tail);
delete_all(From, [H|Tail]) ->  [H|delete_all(From, Tail)];
delete_all(_, []) -> [].

%% do unlink if we have no more references to Pid.
do_unlink(Pid, State) ->
    case lists:member(Pid, State#state.monitor) of
	true ->
	    false;
	_ ->
%	    io:format("unlink(Pid) ~p~n",[Pid]),
	    unlink(Pid)
    end.



%%%====================================================================================
%%% Send a nodeup/down messages to monitoring Pids in the own global group.
%%%====================================================================================
send_monitor([P|T], M, no_conf) -> safesend_nc(P, M), send_monitor(T, M, no_conf);
send_monitor([P|T], M, SyncState) -> safesend(P, M), send_monitor(T, M, SyncState);
send_monitor([], _, _) -> ok.

safesend(Name, {Msg, Node}) when is_atom(Name) ->
    case lists:member(Node, get_own_nodes()) of
	true ->
	    case whereis(Name) of 
		undefined ->
		    {Msg, Node};
		P when is_pid(P) ->
		    P ! {Msg, Node}
	    end;
	false ->
	    not_own_group
    end;
safesend(Pid, {Msg, Node}) -> 
    case lists:member(Node, get_own_nodes()) of
	true ->
	    Pid ! {Msg, Node};
	false ->
	    not_own_group
    end.

safesend_nc(Name, {Msg, Node}) when is_atom(Name) ->
    case whereis(Name) of 
	undefined ->
	    {Msg, Node};
	P when is_pid(P) ->
	    P ! {Msg, Node}
    end;
safesend_nc(Pid, {Msg, Node}) -> 
    Pid ! {Msg, Node}.






%%%====================================================================================
%%% Check which user is associated to the crashed process.
%%%====================================================================================
check_exit(ExitPid, Reason) ->
%    io:format("===EXIT===  ~p ~p ~n~p   ~n~p   ~n~p ~n~n",[ExitPid, Reason, get(registered_names), get(send), get(whereis_name)]),
    check_exit_reg(get(registered_names), ExitPid, Reason),
    check_exit_send(get(send), ExitPid, Reason),
    check_exit_where(get(whereis_name), ExitPid, Reason).


check_exit_reg(undefined, _ExitPid, _Reason) ->
    ok;
check_exit_reg(Reg, ExitPid, Reason) ->
    case lists:keysearch(ExitPid, 1, lists:delete(undefined, Reg)) of
	{value, {ExitPid, From}} ->
	    NewReg = lists:delete({ExitPid, From}, Reg),
	    put(registered_names, NewReg),
	    gen_server:reply(From, {error, Reason});
	false ->
	    not_found_ignored
    end.


check_exit_send(undefined, _ExitPid, _Reason) ->
    ok;
check_exit_send(Send, ExitPid, _Reason) ->
    case lists:keysearch(ExitPid, 1, lists:delete(undefined, Send)) of
	{value, {ExitPid, From, Name, Msg}} ->
	    NewSend = lists:delete({ExitPid, From, Name, Msg}, Send),
	    put(send, NewSend),
	    gen_server:reply(From, {badarg, {Name, Msg}});
	false ->
	    not_found_ignored
    end.


check_exit_where(undefined, _ExitPid, _Reason) ->
    ok;
check_exit_where(Where, ExitPid, Reason) ->
    case lists:keysearch(ExitPid, 1, lists:delete(undefined, Where)) of
	{value, {ExitPid, From}} ->
	    NewWhere = lists:delete({ExitPid, From}, Where),
	    put(whereis_name, NewWhere),
	    gen_server:reply(From, {error, Reason});
	false ->
	    not_found_ignored
    end.



%%%====================================================================================
%%% Kill any possible s_group_check processes
%%%====================================================================================
kill_s_group_check() ->
    case whereis(s_group_check) of
	undefined ->
	    ok;
	Pid ->
	    unlink(Pid),
	    s_group_check ! kill,
	    unregister(s_group_check)
    end.


%%%====================================================================================
%%% Disconnect nodes not belonging to own s_groups
%%%====================================================================================
disconnect_nodes(DisconnectNodes) ->
    lists:foreach(fun(Node) ->
			  {s, Node} ! {disconnect_node, node()},
			  global:node_disconnected(Node)
		  end,
		  DisconnectNodes).


%%%====================================================================================
%%% Disconnect nodes not belonging to own s_groups
%%%====================================================================================
force_nodedown(DisconnectNodes) ->
    lists:foreach(fun(Node) ->
			  erlang:disconnect_node(Node),
			  global:node_disconnected(Node)
		  end,
		  DisconnectNodes).


%%%====================================================================================
%%% Get the current s_groups definition
%%%====================================================================================
get_own_nodes_with_errors() ->
    case application:get_env(kernel, s_groups) of
	undefined ->
	    {ok, all};
	{ok, []} ->
	    {ok, all};
	{ok, NodeGrps} ->
            case catch config_scan(NodeGrps, publish_type) of
		{error, Error} ->
		    {error, Error};
                {ok, OwnSGroups, _} ->
                    Nodes = lists:append([Nodes||{_, _, Nodes}<-OwnSGroups]),
                    {ok, lists:usort(Nodes)}
            end
	    end.

get_own_nodes() ->
    case get_own_nodes_with_errors() of
	{ok, all} ->
	    [];
	{error, _} ->
	    [];
	{ok, Nodes} ->
	    Nodes
    end.


get_own_s_groups_with_nodes() ->
    case application:get_env(kernel, s_groups) of
	undefined ->
	    [];
	{ok, []} ->
	    [];
	{ok, NodeGrps} ->
            case catch config_scan(NodeGrps, publish_type) of
                {error,_Error} ->
                    [];
                {ok, OwnSGroups, _} ->
                    [{Group, Nodes}||{Group, _PubType, Nodes}<-OwnSGroups]
            end
    end.
%%%====================================================================================
%%% -hidden command line argument
%%%====================================================================================
publish_arg() ->
    case init:get_argument(hidden) of
	{ok,[[]]} ->
	    hidden;
	{ok,[["true"]]} ->
	    hidden;
	_ ->
	    normal
    end.


%%%====================================================================================
%%% Own group publication type and nodes
%%%====================================================================================
own_group() ->
    case application:get_env(kernel, s_groups) of
	undefined ->
	    no_group;
	{ok, []} ->
	    no_group;
	{ok, NodeGrps} ->
	    case catch config_scan(NodeGrps, publish_type) of
		{error, _} ->
		    no_group;
                {ok, OwnSGroups, _OtherSGroups} ->
                    NodesDef = lists:append([Nodes||{_, _, Nodes}<-OwnSGroups]),
                    {hidden, NodesDef}   %% All s_groups are hidden?
            end
    end.
 

%%%====================================================================================
%%% Help function which computes publication list
%%%====================================================================================
publish_on_nodes(normal, no_group) ->
    all;
publish_on_nodes(hidden, no_group) ->
    [];
publish_on_nodes(normal, {normal, _}) ->
    all;
publish_on_nodes(hidden, {_, Nodes}) ->
    Nodes;
publish_on_nodes(_, {hidden, Nodes}) ->
    Nodes

%%%====================================================================================
%%% Update net_kernels publication list
%%%====================================================================================
update_publish_nodes(PubArg) ->
    update_publish_nodes(PubArg, no_group).
update_publish_nodes(PubArg, MyGroup) ->
    net_kernel:update_publish_nodes(publish_on_nodes(PubArg, MyGroup)).


%%%====================================================================================
%%% Fetch publication list
%%%====================================================================================
publish_on_nodes() ->
    publish_on_nodes(publish_arg(), own_group()).


%% assume this is no s_group name conflict. HL.
-spec mk_new_s_group_conf(GroupName::group_name(), Nodes::[node()])
                         -> [{group_name(), [node()]}].
mk_new_s_group_conf(GroupName, Nodes0) ->
    Nodes = lists:sort(Nodes0),
    case application:get_env(kernel, s_groups) of
        undefined ->
            [{GroupName, Nodes}];
        {ok, []} ->
             [{GroupName, Nodes}];
        {ok, NodeGroups} ->
            case lists:keyfind(GroupName, 1, NodeGroups) of 
                false ->
                    [{GroupName, Nodes}|NodeGroups];
                _ ->
                    lists:keyreplace(GroupName,1, NodeGroups,{GroupName, Nodes})
            end
    end.

rm_s_group_from_conf(GroupName) ->
    case application:get_env(kernel, s_groups) of
        undefined ->
            [];
        {ok, []} ->
            [];
        {ok, NodeGroups} ->
            [{G, Ns}||{G, Ns}<-NodeGroups, G/=GroupName]
    end.

rm_s_group_nodes_from_conf(GroupName,NodesToRm) ->
    case application:get_env(kernel, s_groups) of
        undefined ->
            [];
        {ok, []} ->
            [];
        {ok, NodeGroups} ->
            case lists:keyfind(GroupName, 1, NodeGroups) of 
                false ->
                    NodeGroups;
                {GroupName, Nodes}->
                    NewGroupNodes = Nodes-- NodesToRm,
                    lists:keyreplace(GroupName,1, NodeGroups,
                                     {GroupName, NewGroupNodes})
            end
    end.

known_s_group_names(Nodes) ->
    lists:foldl(fun(Node, Acc) ->
                        case rpc:call(Node, s_group, s_groups, []) of
                            {badrpc, _} ->
                                [];
                            undefined -> 
                                Acc;
                            {G, Gs} -> 
                                [G|Gs]++Acc
                        end
                end, [], Nodes).
                            
