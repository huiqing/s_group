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
%%    paramemter generators.

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

%% since the type node() is used by Erlang, I use a_node() here.
-record(state, {groups             =[] :: [group()],
                free_groups        =[] ::[free_group()],
                free_hidden_groups =[]  ::[free_hidden_group()],
                nodes              =[]  ::[a_node()]}).  

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

%% We model what we believe the state of the system is 
%% and check whether action on real state has same effect 
%% as on the model.
%%===============================================================
%% Prop
%%===============================================================
prop_statem_machine() ->
   ?FORALL(Cmds,commands(?MODULE),
     begin
         setup(),
         {_H,_S,Result} = run_commands(?MODULE,Cmds),
         teardown(),
         Result==ok
     end).

%%===============================================================
%% eqc callbacks
%%===============================================================
%%---------------------------------------------------------------
%% Returns the state in which each test case starts.
%%---------------------------------------------------------------
%% The number of nodes and the free hidden nodes are fixed here. 
%% Could be more random.

-spec initial_state()->#state{}.
initial_state()->
    NodeNames = [make_node_id(N)
                 ||N<-lists:seq(1,12)],
    Nodes=[{NodeId, [], []}||NodeId<-NodeNames],
    FreeHiddenGroups = [{make_node_id(N), []}||N<-[3, 7, 11]],  
    #state{groups = [], 
           free_groups = [],
           free_hidden_groups=FreeHiddenGroups,
           nodes=Nodes}.

make_node_id(N)->
    list_to_atom("node"++integer_to_list(N)++"@127.0.0.1").
    

%%---------------------------------------------------------------
%% command: generates an appropriate symbolic function call to appear next
%% in a test case, if the symbolic state is S. Test sequences are generated 
%% by using command(S) repeatedly. However, generated calls are only included 
%% in test sequences if their precondition is also true.
%%---------------------------------------------------------------
command(S) ->
    oneof([{call, ?MODULE, register_name, [gen_reg_name(S), 
                                           gen_s_group_name(S), 
                                           gen_pid(S), gen_node_id(S), all_node_ids(S)]},
           {call, ?MODULE, whereis_name,  [gen_node_id(S),
                                           gen_reg_name(S), 
                                           gen_s_group_name(S), all_node_ids(S)]}
          ]).
           

%%---------------------------------------------------------------
%% precondition: returns true if the symbolic call C can be performed 
%% in the state S. Preconditions are used to decide whether or not to 
%% include candidate commands in test cases
%%---------------------------------------------------------------
precondition(_S, {call, ?MODULE, register_name, [_RegName, _SGroupName, _Pid, _CurNode, _AllNodeIds]}) ->
    true;
precondition(_S, {call, ?MODULE, whereis_name, [_NodeId, _RegName, _SGroupName, _CurNode, _AllNodeIds]}) ->
    true;
precondition(_S, _C) ->
    true.


%%---------------------------------------------------------------
%% Checks the postcondition of symbolic call C, executed in dynamic state S, 
%% with result R. The arguments of the symbolic call are the actual values passed, 
%% not any symbolic expressions from which they were computed. Thus when a postcondition 
%% is checked, we know the function called, the values it was passed, the value it returned, 
%% and the state in which it was called. Of course, postconditions are checked during 
%% test execution, not test generation.
%%---------------------------------------------------------------
%% Here the state 'S' is the state before the call.
postcondition(S,  {call, ?MODULE, register_name, [RegName, SGroupName, Pid, _CurNode, _AllNodeIds]},
              {Res, ActualState}) ->
    Grps=S#state.groups,
    case lists:keyfind(SGroupName,1, Grps) of 
        {SGroupName, NodeIds, NameSpace} ->
            case lists:keyfind(RegName, 1, NameSpace) of 
                {RegName, _} ->
                    element(1, Res)==error andalso
                        is_the_same(ActualState,S);
                false ->
                    case lists:keyfind(Pid,2,NameSpace) of 
                        {_, Pid} -> element(1, Res)==error;
                        false ->
                            NewGrp={SGroupName,NodeIds, [{RegName, Pid}|NameSpace]},
                            NewGrps = lists:keyreplace(SGroupName, 1, Grps, NewGrp),
                            NewS=S#state{groups=NewGrps},
                            Res==ok andalso is_the_same(ActualState,NewS) 
                                andalso prop_partition(NewS);
                        _ -> 
                            element(1, Res)==error andalso 
                                is_the_same(ActualState, S)
                    end
            end;
        false -> element(1, Res)==error  andalso
                     is_the_same(ActualState,S)
    end;
postcondition(S, {call, ?MODULE, whereis_name, _Args},{_Res, ActualState}) ->
   %%TODO: add stuff here.
    is_the_same(ActualState, S);
postcondition(_S, _C, _R) ->
    true.


%%---------------------------------------------------------------
%% This is the state transition function of the abstract state machine, 
%% and it is used during both test generation and test execution.
%%---------------------------------------------------------------
%%-spec(next_state(S::#state{}, R::var(), C::call()) -> #state{}).
next_state(S, _V, {call, ?MODULE, register_name, 
                   [RegName, SGroupName, Pid, _CurNode, _AllNodeIds]}) ->
    #state{groups=Grps}=S,
    case lists:keyfind(SGroupName, 1, Grps) of 
        {SGroupName, NodeIds, NameSpace} -> 
            case lists:keyfind(RegName, 1, NameSpace) of 
                {RegName, _} ->
                    S;
                false ->
                    case lists:keyfind(Pid,2,NameSpace) of 
                        {_, Pid} -> S;
                        false ->
                            NewGrp={SGroupName,NodeIds, [{RegName, Pid}|NameSpace]},
                            NewGrps = lists:keyreplace(SGroupName, 1, Grps, NewGrp),
                            S#state{groups=NewGrps};
                        _ -> S
                    end
            end;
        false -> S
    end;
next_state(S, _V, {call, ?MODULE, whereis_name, _Args}) ->
    S;
next_state(S, _V, _) ->
    S.


%%-------------------------------------------------------------%%
%%                                                             %%
%%                  General Properties                         %%
%%                                                             %%
%%-------------------------------------------------------------%%
prop_partition(S) ->
    #state{groups=Grps, free_groups = Fgs,free_hidden_groups=Fhgs,nodes=Nodes}=S,
    GrpNodes = sets:from_list(lists:append([NodeIds||{_,NodeIds,_}<-Grps])),
    FreeNodes = sets:from_list(lists:append([NodeIds||{NodeIds,_}<-Fgs])),
    FreeHiddenNodes=sets:from_list([NodeId||{NodeId, _NameSpace}<-Fhgs]),
    AllNodeIds = [NodeId||{NodeId, _, _}<-Nodes],
    Empty = sets:new(),
    sets:intersection(GrpNodes, FreeNodes)==Empty andalso
        sets:intersection(GrpNodes, FreeHiddenNodes)==Empty andalso
        sets:interaction(FreeNodes, FreeHiddenNodes)==Empty andalso
        sets:union([GrpNodes, FreeNodes, FreeHiddenNodes])== 
        sets:from_list(AllNodeIds).
 
%%---------------------------------------------------------------
%%
%%  Adaptor functions.
%%---------------------------------------------------------------
register_name(RegName, SGroupName, Pid, Node, AllNodes) ->
    Res=rpc:call(Node, global, register_name, [RegName,SGroupName, Pid]),
    State=fetch_node_states(AllNodes),
    {Res, State}.
    
  
whereis_name(NodeId, RegName, SGroupName, Node, AllNodes)->
    Res=rpc:call(Node, global, whereis_name, [NodeId, RegName, SGroupName]),
    State =fetch_node_states(AllNodes),
    {Res, State}.


%%---------------------------------------------------------------
%%
%%  translate real state to model state
%%---------------------------------------------------------------
fetch_node_states(Nodes) ->
    NodeIds = [NodeId||{NodeId, _, _}<-Nodes],
    [{NodeId, rpc:call(NodeId, s_group, info, [])}||NodeId<-NodeIds].

to_model(NodeIdStatePairs) ->
    GroupNodes = [{NodeId, lists:keyfind(own_grps, 1, State)} 
                  ||{NodeId, State}<-NodeIdStatePairs, 
                    lists:member({state, synced}, State)],
    Groups = analyze_group_nodes(GroupNodes),
    FreeNodes = [{NodeId, fetch_name_space(NodeId)}
                 ||{NodeId, State}<-NodeIdStatePairs, 
                   lists:member({state, no_conf}, State), 
                   lists:memebr({publish_type, normal}, State)],
    FreeGroups = analyze_free_nodes(FreeNodes),
    FreeHiddenGroups = [{NodeId, fetch_name_space(NodeId)}
                        ||{NodeId, State}<-NodeIdStatePairs, 
                          lists:member({state, no_conf}, State), 
                          lists:memebr({publish_type, hidden}, State)],
    AllNodes=[{NodeId, connections(NodeId), group_names(State)}
           ||{NodeId, State}<-NodeIdStatePairs],
    #state{ groups = Groups,
            free_groups = FreeGroups,
            free_hidden_groups=FreeHiddenGroups,
            nodes=AllNodes}.
    
group_names(State) -> 
    case lists:keyfind(own_grps, 1, State) of 
        false -> [];
        {own_grps, OwnGrps} ->
            {GroupNames,_} =lists:unzip(OwnGrps),
            GroupNames
    end.
connections(NodeId) ->
    Visibles=rpc:call(NodeId, erlang, nodes, [visible]),
    Hiddens =rpc:call(NodeId, erlang, nodes, [hidden]),
    [{Id, visible}||Id<-Visibles] ++ 
        [{Id, hidden}||Id<-Hiddens].
                    
%% function registered_names_with_pids is not defined in 
%% global.erl at the moment; to be added.
fetch_name_space(NodeId) ->
    NameSpace=rpc:call(NodeId,global, registered_names_with_pids, []),
    lists:sort(NameSpace).

analyze_free_nodes(NodeIdNameSpacePairs)->
    %% need to take the connections into account!  
    group_by(2, NodeIdNameSpacePairs).  

analyze_group_nodes(GroupNameNodesPairs) ->
    GroupNameNodesPairs1=sets:to_list(
                           sets:from_list(
                             lists:append(GroupNameNodesPairs))),
    [{GroupName, Nodes, lists:sort(
                          lists:append(
                            [fetch_name_space(NodeId)
                             ||NodeId<-Nodes]))}
     ||{GroupName, Nodes}<-GroupNameNodesPairs1].

is_the_same(State, Model) ->
    normalise_model(to_model(State))==
        normalise_model(Model).

normalise_model(Model) ->
    Groups = Model#state.groups,
    FreeGroups = Model#state.free_groups,
    FreeHiddenGroups = Model#state.free_hidden_groups,
    Nodes = Model#state.nodes,
    Groups1=lists:keysort(1, [{GrpName, lists:usort(NodeIds), lists:usort(NameSpace)}
                              ||{GrpName, NodeIds, NameSpace}<-Groups]),
    FreeGroups1 = lists:keysort(1, [{lists:usort(Ids), lists:usort(NameSpace)}
                                    ||{Ids, NameSpace}<-FreeGroups]),
    FreeHiddenGroups1 = lists:keysort(1,[{Id, lists:usort(NameSpace)}
                                        ||{Id, NameSpace}<-FreeHiddenGroups]),
    Nodes1 = lists:keysort(1, [{Id, lists:usort(Conns), lists:usort(GrpNames)}
                              ||{Id, Conns, GrpNames}<-Nodes]),
    #state{groups = Groups1, 
           free_groups = FreeGroups1,
           free_hidden_groups = FreeHiddenGroups1,
           nodes = Nodes1}.
           
    


                          
%%---------------------------------------------------------------
%%
%% Generators.
%%---------------------------------------------------------------
gen_reg_name(_S)->
    ok.

gen_pid(_S) -> ok.

gen_node_id(_S) -> ok.

gen_s_group_name(_S) -> ok.

gen_node_ids(_S) -> ok.


%%---------------------------------------------------------------
%%
%%   Utility functions.
%%
%%--------------------------------------------------------------
%% start nodes...
setup()->
    ok.

teardown()->
    ok.

all_node_ids(S) ->
    [NodeId||{NodeId, _, _}<-S#state.nodes].

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
    group_by(N,TupleList2,Acc ++ [TupleList1]).
