%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                                                      %%
%%  DISCLAIMER:  This is work in progress.              %%
%%                                                      %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% The module is for testing s_group APIs using QuickCheck.
%% The node on which this test in run should start as a 
%% hidden node.

%% Things to add:
%%    precondtions for each command. 

-module(s_group_eqc).

-include_lib("eqc/include/eqc.hrl").

-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

%% eqc callbacks
-export([initial_state/0, 
         command/1,
         precondition/2,
         postcondition/3,
         next_state/3]).

-export([prop_s_group/0,
         register_name/2,
         whereis_name/2,
         send/2]).

%% since the type node() is used by Erlang, I use a_node() here.
-record(model, {groups             =[] :: [group()],
                free_groups        =[] ::[free_group()],
                free_hidden_groups =[]  ::[free_hidden_group()],
                nodes              =[]  ::[a_node()]}).  

-record(state, {ref  =[]  ::[{pid(),[node_id()],[tuple()]}], 
                model     ::#model{}
               }).

-type group()::{s_group_name(), [node_id()], namespace()}.
-type s_group_name()::atom().
-type node_id()::node().
-type namespace()::[{atom(), pid()}].

-type free_group()::{[node_id()], namespace()}.

-type free_hidden_group()::{node_id(), namespace()}. 
  
-type a_node()::{node_id(), connections(), gr_names()}.
-type gr_names()::free_normal_group|free_hidden_group|[s_group_name()].

-type connections()::[connection()].
-type connection()::{node_id(), connection_type()}.
-type connection_type()::visible|hidden.


-define(debug, 9).

%% -define(debug, -1).
-ifdef(debug). 
dbg(Level, F, A) when Level >= ?debug ->
    io:format(F, A),
    ok;
dbg(_, _, _) ->
    ok.
-define(dbg(Level, F, A), dbg((Level), (F), (A))).
-else.
-define(dbg(Level, F, A), ok).
-endif.


%% We model what we believe the state of the system is 
%% and check whether action on real state has same effect 
%% as on the model.
%%===============================================================
%% Prop
%%===============================================================
prop_s_group() ->
    ?SETUP(
       fun setup/0,
       ?FORALL(Cmds,commands(?MODULE),
               begin
                   {H,S,Res} = run_commands(?MODULE,Cmds),
                   pretty_commands(?MODULE, Cmds, {H,S,Res}, (Res==yes orelse Res==ok))
               end)).

%%===============================================================
%% eqc callbacks
%%===============================================================
%%---------------------------------------------------------------
%% Returns the state in which each test case starts.
%%---------------------------------------------------------------
%% The number of nodes and the free hidden nodes are fixed here. 
%% Could be more random.
%% Here is the configuration file used.
%% [{kernel, 
%%   [{s_groups,
%%    [{group1, normal,  ['node1@127.0.0.1', 'node2@127.0.0.1',
%%                        'node3@127.0.0.1','node4@127.0.0.1']},
%%     {group2, normal,  ['node3@127.0.0.1', 'node4@127.0.0.1',
%%                        'node5@127.0.0.1', 'node6@127.0.0.1']},
%%     {group3, normal,  ['node4@127.0.0.1',  'node6@127.0.0.1',
%%                        'node7@127.0.0.1',  'node8@127.0.0.1']
%%     }]}]}].

-spec initial_state()->#state{}.
initial_state()->
    NodeIds = [make_node_id(N)||N<-lists:seq(1,14)],
    
    FreeHiddenGrps = [{make_node_id(N), []}
                      ||N<-[9, 10]],  
    FreeNormalGrps = [{[make_node_id(N)], []}
                      ||N<-[11, 12,13,14]],
    {ok, [Config]} = file:consult("s_group.config"),
    {kernel, Kernel}=lists:keyfind(kernel, 1, Config),
    {s_groups, Grps} = lists:keyfind(s_groups, 1, Kernel),
    SGrps = [{Name, Nids, []}||{Name, _, Nids}<-Grps],
    Nodes=[{NodeId, connections(NodeId), 
            [Name||{Name, _, Nids}<-Grps, 
                   lists:member(NodeId, Nids)]}||NodeId<-NodeIds],
    NodeStates=fetch_node_states(NodeIds),
    Model=#model{groups = SGrps, 
                 free_groups = FreeNormalGrps,
                 free_hidden_groups=FreeHiddenGrps,
                 nodes=Nodes},
    #state{ref=NodeStates, model=Model}.

make_node_id(N)->
    list_to_atom("node"++integer_to_list(N)++"@127.0.0.1").
    

%%---------------------------------------------------------------
%% command: generates an appropriate symbolic function call to appear next
%% in a test case, if the symbolic state is S. Test sequences are generated 
%% by using command(S) repeatedly. However, generated calls are only included 
%% in test sequences if their precondition is also true.
%%---------------------------------------------------------------
command(S) ->
    oneof([{call, ?MODULE, register_name,[gen_register_name_pars(S), all_node_ids(S)]}
           %% ,{call, ?MODULE, whereis_name, [gen_whereis_name_pars(S),  all_node_ids(S)]}
           %% ,{call, ?MODULE, send,[gen_send_pars(S),all_node_ids(S)]}
          ]).
 
%%---------------------------------------------------------------
%% precondition: returns true if the symbolic call C can be performed 
%% in the state S. Preconditions are used to decide whether or not to 
%% include candidate commands in test cases
%%---------------------------------------------------------------
precondition(_S, {call, ?MODULE, register_name,
                  [{RegName, _SGroupName, _Pid,_CurNode}, _AllNodeIds]}) ->
    ?dbg(0, "Cmd:~p\n", [{call, ?MODULE, register_name,
                       [{RegName, _SGroupName, _Pid,_CurNode}]}]),
    RegName/=undefined;
precondition(S, {call, ?MODULE, whereis_name, 
                 [{_NodeId, _RegName, _SGroupName, 
                   _CurNode}, _AllNodeIds]}) ->
    Model = S#state.model,
    Grps = Model#model.groups,
    Grps/=[]; 
precondition(S, {call, ?MODULE, send, 
                  [{_NodeId, _RegName, _SGroupName, _Msg, 
                   _CurNode}, _AllNodeIds]}) ->
    Model = S#state.model,
    Grps = Model#model.groups,
    Grps /=[];
precondition(_S, _C) ->
    true.

%%---------------------------------------------------------------
%% Checks the postcondition of symbolic call C, executed in 
%% dynamic state S, 
%% with result R. The arguments of the symbolic call are the actual 
%% values passed, not any symbolic expressions from which they were 
%% computed. Thus when a postcondition is checked, we know the function 
%% called, the values it was passed, the value it returned, 
%% and the state in which it was called. Of course, postconditions are 
%% checked during test execution, not test generation.
%%---------------------------------------------------------------
%% Here the state 'S' is the state before the call.
postcondition(S,  {call, ?MODULE, register_name, 
                   [{RegName, SGroupName, Pid, _CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    ?dbg(0,"check register_name postcondtion ...\n",[]),
    Model = S#state.model,
    Grps=Model#model.groups,
    case lists:keyfind(SGroupName,1, Grps) of 
        {SGroupName, NodeIds, NameSpace} ->
            case lists:keyfind(RegName, 1, NameSpace) of 
                {RegName, _} ->
                    Res==no andalso
                        is_the_same(ActualState,S);
                false ->
                    case lists:keyfind(Pid,2,NameSpace) of 
                        {_, Pid} ->
                            Res==no andalso is_the_same(ActualState,S);
                        false ->
                            NewGrp={SGroupName,NodeIds, [{RegName, Pid}|NameSpace]},
                            NewGrps = lists:keyreplace(SGroupName, 1, Grps, NewGrp),
                            NewModel =Model#model{groups=NewGrps},
                            NewS=S#state{model=NewModel},
                            Res==yes andalso is_the_same(ActualState,NewS) 
                                andalso prop_partition(NewS);
                        _ -> 
                            Res==no andalso 
                                is_the_same(ActualState, S)
                    end
            end;
        false ->
            Res==no  andalso
                is_the_same(ActualState,S)
    end;
postcondition(S, {call, ?MODULE, whereis_name, 
                   [{TargetNodeId, RegName, GroupName, CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    Pid = find_name(S#state.model, TargetNodeId, GroupName,RegName), 
    NewS=whereis_name_next_state(S, CurNode, TargetNodeId),
    Pid == Res andalso is_the_same(ActualState, NewS);   
postcondition(_S, {call, ?MODULE, send, 
                   [{_NodeId, _RegName, _SGroupName, _Msg,_CurNode}, _AllNodeIds]},
              {_Res, _ActualState}) ->
    true;
postcondition(_S, _C, _R) ->
    true.


%%---------------------------------------------------------------
%% This is the state transition function of the abstract state machine, 
%% and it is used during both test generation and test execution.
%%---------------------------------------------------------------
%%-spec(next_state(S::#state{}, R::var(), C::call()) -> #state{}).
next_state(S, _V, {call, ?MODULE, register_name, 
                   [{RegName, SGroupName, Pid, _CurNode}, _AllNodeIds]}) ->
    Model = S#state.model,
    #model{groups=Grps}=Model,
    case lists:keyfind(SGroupName, 1, Grps) of 
        {SGroupName, NodeIds, NameSpace} -> 
            case lists:keyfind(RegName, 1, NameSpace) of 
                {RegName, _} ->
                    S;
                false ->
                    case lists:keyfind(Pid,2,NameSpace) of 
                        {_, Pid} -> S;
                        false ->
                            NewGrp={SGroupName,NodeIds, 
                                    [{RegName, Pid}|NameSpace]},
                            NewGrps = lists:keyreplace(SGroupName, 1, 
                                                       Grps, NewGrp),
                            S#state{model=Model#model{groups=NewGrps}};
                        _ -> S
                    end
            end;
        false -> S
    end;
next_state(S, _V, {call, ?MODULE, whereis_name,  
                   [{TargetNode, _RegName, _SGroupName,
                     CurNode},_AllNodeIds]}) ->
    whereis_name_next_state(S, CurNode, TargetNode);

next_state(S, _V, {call, ?MODULE, send,  
                   [{_NodeId, _RegName, _SGroupName, _Msg,
                     _CurNode}, _AllNodeIds]}) ->
    S;
next_state(S, _V, _) ->
    S.

whereis_name_next_state(S, CurNode, TargetNode) ->
    #model{nodes=Nodes}=Model=S#state.model,
    {CurNode, CurConns, CurGrps} = lists:keyfind(CurNode, 1, Nodes),
    {TargetNode, _TargetConns, TargetGrps} = lists:keyfind(TargetNode, 1, Nodes),
    case lists:keyfind(TargetNode, 1, CurConns) of
        {TargetNode, _} -> S;  %% a connection exists.
        false ->
            case CurGrps == [free_normal_group] andalso
                TargetGrps == [free_normal_group] of
                true ->
                    %% both are free nodes, but not in the same group 
                    %% (otherwise should be connected already).
                    NewModel=merge_two_free_groups(Model, CurNode, TargetNode),
                    S#state{model=NewModel};
                false ->
                    %% all the other cases.
                    NewNodes=add_hidden_connections(Nodes, CurNode, TargetNode),
                    NewModel=Model#model{nodes=NewNodes},
                    S#state{model=NewModel}
            end
    end.

%%-------------------------------------------------------------%%
%%                                                             %%
%%                  General Properties                         %%
%%                                                             %%
%%-------------------------------------------------------------%%
prop_partition(S) ->
    #model{groups=Grps, free_groups = Fgs,
           free_hidden_groups=Fhgs,nodes=Nodes}=S#state.model,
    GrpNodes = sets:from_list(
                 lists:append(
                   [NodeIds||{_,NodeIds,_}<-Grps])),
    FreeNodes = sets:from_list(
                  lists:append(
                    [NodeIds||{NodeIds,_}<-Fgs])),
    FreeHiddenNodes=sets:from_list(
                      [NodeId||{NodeId, _NameSpace}<-Fhgs]),
    AllNodeIds = [NodeId||{NodeId, _, _}<-Nodes],
    Empty = sets:new(),
    sets:intersection(GrpNodes, FreeNodes)==Empty andalso
        sets:intersection(GrpNodes, FreeHiddenNodes)==Empty andalso
        sets:interaction(FreeNodes, FreeHiddenNodes)==Empty andalso
        sets:union([GrpNodes, FreeNodes, FreeHiddenNodes])== 
        sets:from_list(AllNodeIds).

%% TO ADD:
%% Property about connections: if A is connected to B, then B should be connected to A too.
%% Namespace: no conficits in namespace.
%% The nodeids in a group should not be empty.
%%---------------------------------------------------------------
%%
%%  Adaptor functions.
%%---------------------------------------------------------------
register_name({RegName, SGroupName, Pid, Node}, AllNodes) ->
    Res=rpc:call(Node, s_group, register_name, 
                 [RegName,SGroupName, Pid]),
    State=fetch_node_states(AllNodes),
    {Res, State}.
    
  
whereis_name({NodeId, RegName, SGroupName, Node}, AllNodes)->
    Res=rpc:call(Node, s_group, whereis_name,
                 [NodeId, RegName, SGroupName]),
    State =fetch_node_states(AllNodes),
    {Res, State}.

send({NodeId, RegName, SGroupName, Msg, Node}, AllNodes)->
    Res=rpc:call(Node, s_group, send,
                 [NodeId, RegName, SGroupName, Msg]),
    State =fetch_node_states(AllNodes),
    {Res, State}.

%%---------------------------------------------------------------
%%
%%  translate real state to model state
%%---------------------------------------------------------------
fetch_node_states(_Nodes) ->
    NodeIds=[list_to_atom("node"++integer_to_list(N)++"@127.0.0.1")
             ||N<-lists:seq(1,14)],
    %% NodeIds = [NodeId||{NodeId, _, _}<-Nodes],
    %% this s_group info should also return pids.
    [{NodeId, rpc:call(NodeId, erlang, processes, []),
      rpc:call(NodeId, s_group, info, [])}||NodeId<-NodeIds].

to_model(NodeIdStatePairs) ->
    ?dbg(0, "actual to abstract model.....\n",[]),
    GroupNodes0 =[case lists:keyfind(own_s_groups, 1, State) of
                     {own_s_groups, []}-> [];
                     {own_s_groups, List} -> List
                 end                          
                  ||{_NodeId, _, State}<-NodeIdStatePairs, 
                   lists:member({state, synced}, State)],
    GroupNodes = sets:to_list(sets:from_list(lists:append(GroupNodes0))),
    ?dbg(0, "GroupNodes:~p\n",[GroupNodes]),
    Groups = analyze_group_nodes(GroupNodes),
    ?dbg(0, "Groups:~p\n", [Groups]),
    FreeNodes = [{NodeId, [NodeId|connections(NodeId)], 
                  fetch_name_space(NodeId)}
                 ||{NodeId, _Pids,State}<-NodeIdStatePairs, 
                   lists:member({own_s_groups, []}, State), 
                   lists:member({publish_type, normal}, State)],
    ?dbg(0, "FreeNode:~p\n", [FreeNodes]),
    FreeGroups = analyze_free_nodes(FreeNodes),
    ?dbg(0, "FreeGroups:~p\n", [FreeGroups]),
    FreeHiddenGroups = [{NodeId, fetch_name_space(NodeId)}
                        ||{NodeId, State}<-NodeIdStatePairs,
                          lists:member({own_s_groups, []}, State),
                          lists:memebr({publish_type, hidden}, State)],
    AllNodes=[{NodeId, connections(NodeId), group_names(State)}
           ||{NodeId, _, State}<-NodeIdStatePairs],
    ?dbg(0,"AllNodes:~p\n", [AllNodes]),
    #model{groups = Groups,
           free_groups = FreeGroups,
           free_hidden_groups=FreeHiddenGroups,
           nodes=AllNodes}.
    
group_names(State) -> 
    case lists:keyfind(own_s_groups, 1, State) of 
        false -> [];
        {own_s_groups, OwnGrps} ->
            {GroupNames,_} =lists:unzip(OwnGrps),
            GroupNames
    end.
connections(NodeId) ->
    Visibles=rpc:call(NodeId, erlang, nodes, [visible]),
    Hiddens =rpc:call(NodeId, erlang, nodes, [hidden]),
    [{Id, visible}||Id<-Visibles] ++ 
        [{Id, hidden}||Id<-Hiddens--['eqc@127.0.0.1']].
                    
%% function registered_names_with_pids is not defined in 
%% global.erl at the moment; to be added.
fetch_name_space(NodeId) ->
    NameSpace=rpc:call(NodeId,global, registered_names_with_pids, []),
    ?dbg(0,"NameSpace:~p\n", [NameSpace]),
    lists:sort(NameSpace).

analyze_free_nodes(FreeNodes)->
    FreeNodeIds = [NodeId||{NodeId, _Conns, _NameSpace}<-FreeNodes],
    NodeIdWithConnsAndNameSpace=
        [{NodeId, {FreeNodeIds--(FreeNodeIds--Conns), NameSpace}}||
            {NodeId, Conns, NameSpace}<-FreeNodes],
    %% need to take the connections into account!  
    FreeGroups= group_by(2, NodeIdWithConnsAndNameSpace),
    [{NodeIds, NameSpace}||{NodeIds, {_Conns, NameSpace}}<-FreeGroups].

%% This should be more strict!!!
analyze_group_nodes(GroupNameNodesPairs) ->
    F = fun(NodeIds, GrpName) ->
                NameSpace=[[{Name, Pid}||{Name, Grp, Pid}<-
                                             fetch_name_space(Id), Grp==GrpName]
                           || Id<-NodeIds],
                sets:to_list(sets:from_list(lists:append(NameSpace)))
        end,
    [{GroupName, Nodes,  F(Nodes, GroupName)}||{GroupName, Nodes}<-GroupNameNodesPairs].

is_the_same(State, AbstractState) ->
    ActualModel =normalise_model(to_model(State)),
    AbstractModel=normalise_model(AbstractState#state.model),
    ?dbg(0, "SameNodes:~p\n", [ActualModel#model.nodes==AbstractModel#model.nodes]),
    ?dbg(0, "SameSGroups:~p\n", [ActualModel#model.groups==AbstractModel#model.groups]),
    ?dbg(0, "ActualModel:~p\n", [ActualModel]),
    ?dbg(0, "AbstracrModel:~p\n", [AbstractModel]),
    ActualModel==AbstractModel.

normalise_model(Model) ->
    Groups = Model#model.groups,
    FreeGroups = Model#model.free_groups,
    FreeHiddenGroups = Model#model.free_hidden_groups,
    Nodes = Model#model.nodes,
    Groups1=lists:keysort(
              1, [{GrpName, lists:usort(NodeIds), 
                   lists:usort(NameSpace)}
                  ||{GrpName, NodeIds, NameSpace}<-Groups]),
    FreeGroups1 = lists:keysort(
                    1, [{lists:usort(Ids), lists:usort(NameSpace)}
                        ||{Ids, NameSpace}<-FreeGroups]),
    FreeHiddenGroups1 = lists:keysort(
                          1,[{Id, lists:usort(NameSpace)}
                             ||{Id, NameSpace}<-FreeHiddenGroups]),
    Nodes1 = lists:keysort(
               1, [{Id, lists:usort(Conns), lists:usort(GrpNames)}
                   ||{Id, Conns, GrpNames}<-Nodes]),
    #model{groups = Groups1, 
           free_groups = FreeGroups1,
           free_hidden_groups = FreeHiddenGroups1,
           nodes = Nodes1}.
           
                         
%%---------------------------------------------------------------
%%
%% Generators.
%%---------------------------------------------------------------
%% How to solve the dependency between parameters?
gen_register_name_pars(_S=#state{ref=Ref, model=Model}) ->
    Grps=Model#model.groups,
    if Grps==[] orelse Ref==[] ->
           {undefined, undefined, undefined, undefined};
       true ->
            ?LET({GrpName, NodeIds, _Namespace}, eqc_gen:oneof(Grps),
                 ?LET(NodeId, eqc_gen:oneof(NodeIds),
                      ?LET(Name, gen_reg_name(),
                           {list_to_atom(Name),GrpName, 
                            oneof(element(2, lists:keyfind(NodeId, 1, Ref))),
                            NodeId})))
    end.

gen_whereis_name_pars(_S=#state{model=Model}) ->
    Grps=Model#model.groups,
    FreeGrps = [{free_normal_group, Ids, NS}||
                   {Ids, NS}<-Model#model.free_groups],
    HiddenGrps=[{free_hidden_group, [Id], NS}||
                   {Id, NS}<-Model#model.free_hidden_groups],
    AllGrps = Grps++FreeGrps++HiddenGrps,
    ?LET({GrpName, NodeIds, NameSpace},
         eqc_gen:oneof(AllGrps),
         {eqc_gen:oneof(NodeIds), 
          eqc_gen:oneof(element(1,lists:unzip(NameSpace))),
          GrpName}).

gen_send_pars(_S=#state{ref=_Ref, model=Model}) ->
    Grps=Model#model.groups,
    FreeGrps = [{free_normal_group, Ids, NS}||
                   {Ids, NS}<-Model#model.free_groups],
    HiddenGrps=[{free_hidden_group, [Id], NS}||
                   {Id, NS}<-Model#model.free_hidden_groups],
    AllGrps = Grps++FreeGrps++HiddenGrps,
    ?LET({GrpName, NodeIds, NameSpace},
         eqc_gen:oneof(AllGrps),
         {eqc_gen:oneof(NodeIds),
          eqc_gen:oneof(element(1,lists:unzip(NameSpace))),
          GrpName, gen_message()}).

gen_reg_name()->
    eqc_gen:non_empty(eqc_gen:list(eqc_gen:choose(97, 122))).
   
gen_message() ->
    eqc_gen:binary().
%%---------------------------------------------------------------
%%
%%   Utility functions.
%%
%%--------------------------------------------------------------
%% start nodes...
setup()->
    fun teardown/0.

teardown()->
    F=fun(N) ->
              Node=list_to_atom("node"++integer_to_list(N)++"@127.0.0.1"),
              rpc:call(Node, erlang, halt, [])
      end,
    lists:foreach(fun(N) -> F(N) end, lists:seq(1, 14)).
   

all_node_ids(S) ->
    [NodeId||{NodeId, _, _}<-S#state.model#model.nodes].

add_hidden_connections(Nodes, Node1, Node2) ->
    {Node1, Conns1, Grps1} = lists:keyfind(Node1,1, Nodes),
    {Node2, Conns2, Grps2} = lists:keyfind(Node2,1, Nodes),
    NewConns1 = [{Node2, hidden}|Conns1],
    NewConns2 = [{Node1, hidden}|Conns2],
    lists:keyreplace(Node2, 1, 
                     lists:keyreplace(Node1, 1, Nodes, 
                                      {Node1, NewConns1, Grps1}),
                     {Node2, NewConns2, Grps2}).

merge_two_free_groups(Model=#model{free_groups=FreeGrps, nodes=Nodes}, 
                      Node1, Node2) ->
    [FreeGrp1={NodeIds1, NameSpace1}] =
        [{NodeIds, NameSpace}||{NodeIds, NameSpace}<-FreeGrps, 
                               lists:member(Node1, NodeIds)],
    [FreeGrp2={NodeIds2, NameSpace2}] =
        [{NodeIds, NameSpace}||{NodeIds, NameSpace}<-FreeGrps, 
                               lists:member(Node2, NodeIds)],
    OtherGrps = FreeGrps -- [FreeGrp1, FreeGrp2],
    %%NOTE: WE ASSUME THAT THIS IN NO NAME CONFLICTION.
    NewFreeGrp ={NodeIds1++NodeIds2, NameSpace1++NameSpace2}, 
    NewFreeGrps=[NewFreeGrp|OtherGrps],
    NewNodes = add_visible_connections(Nodes, NodeIds1, NodeIds2),
    Model#model{free_groups=NewFreeGrps, nodes=NewNodes}.
    

add_visible_connections(AllNodes, NodeIds1, NodeIds2) ->
    F = fun(Node={NodeId, Conns, GrpNames}) ->
                case lists:member(NodeId, NodeIds1) of 
                    true ->
                        Conns1=[{Id, visible}||Id<-NodeIds2],
                        {NodeId, Conns1++Conns, GrpNames};
                    false ->
                        case lists:memebr(NodeId, NodeIds2) of 
                            true ->
                                Conns2=[{Id, visible}||Id<-NodeIds1],
                                {NodeId, Conns2++Conns, GrpNames};
                            false -> Node
                        end
                end
        end,                                    
    [F(Node)||Node<-AllNodes].
    
find_name(Model, NodeId, GroupName, RegName) ->
    Nodes = Model#model.nodes,
    {NodeId, _, Grps} = lists:keyfind(NodeId,1,Nodes),
    [NameSpace]=case Grps of 
                    [free_hidden_group] ->
                        FreeHiddenGrps=Model#model.free_hidden_groups,
                        [NS||{Id, NS}<-FreeHiddenGrps, Id==NodeId];
                    [free_normal_group] ->
                        FreeGrps = Model#model.free_groups,
                        [NS||{Ids, NS}<-FreeGrps, lists:member(NodeId, Ids)];
                    _ ->
                        Grps = Model#model.groups,
                        [NS||{GrpName, _Ids, NS}<-Grps, GrpName==GroupName]
                end,
    case lists:keyfind(RegName, 1, NameSpace) of 
        {RegName, Pid} -> Pid;  %% Note: this pid may not have the node info!
        _ -> undefined
    end.
    
%%---------------------------------------------------------------
%%
%%   Miscellaneous functions.
%%
%%--------------------------------------------------------------

-spec group_by(integer(), [tuple()]) -> [[tuple()]].
group_by(N, TupleList) ->
    SortedTupleList = lists:keysort(N, lists:usort(TupleList)),
    group_by(N, SortedTupleList, []).

group_by(_N,[],Acc) -> Acc;
group_by(N,TupleList = [T| _Ts],Acc) ->
    E = element(N,T),
    {TupleList1,TupleList2} = 
	lists:partition(fun (T1) ->
				element(N,T1) == E
			end,
			TupleList),
    {Es,_} = lists:unzip(TupleList1),
    group_by(N,TupleList2,Acc ++ [{Es, E}]).

%% cmd to start testing:
%% qc:quickcheck(s_group_eqc:prop_s_group()). 
