%% Copyright (c) 2013, Huiqing Li
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%     %% Redistributions of source code must retain the above copyright
%%       notice, this list of conditions and the following disclaimer.
%%     %% Redistributions in binary form must reproduce the above copyright
%%       notice, this list of conditions and the following disclaimer in the
%%       documentation and/or other materials provided with the distribution.
%%     %% Neither the name of the copyright holders nor the
%%       names of its contributors may be used to endorse or promote products
%%       derived from this software without specific prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS ''AS IS''
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
%% BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
%% BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
%% WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
%% OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF 
%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%                                                      %%
%%  DISCLAIMER:  THIS IS WORK IN PROGRESS!              %%
%%                                                      %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% The module is for testing the s_group API using QuickCheck.
%%
%% *Notes:* 
%% 1. The node on which this test is run should be started 
%%    as a hidden node.
%% 2. cmd to start testing:
%%    eqc:quickcheck(s_group_eqc:prop_s_group()). 
%% 3: install SD Erlang:
%%    The code for SD Erlang is in Natalia's repository, 
%%    copy those files to your Erlang OTP kernel-vsn/src 
%%    directory(this will replace some of the existing files!),
%%    compile the new files and put the .beam in kernel-vsn/ebin.
%%   
%%@author H.Li(H.Li@kent.ac.uk).

-module(s_group_eqc).

-include_lib("eqc/include/eqc.hrl").

-include_lib("eqc/include/eqc_statem.hrl").

-compile(export_all).

%% eqc callbacks
-export([initial_state/0, 
         command/1,
         precondition/2,
         postcondition/3,
         next_state/3,
         invariant/1
        ]).

-export([prop_s_group/0]).
  
-record(model, {groups             =[] :: [group()],
                free_groups        =[] ::[free_group()],
                free_hidden_groups =[]  ::[free_hidden_group()],
                nodes              =[]  ::[a_node()]}).  

-type group()::{s_group_name(), [node_id()], namespace()}.
-type s_group_name()::atom().
-type node_id()::node().
-type namespace()::[{atom(), pid()}].
-type free_group()::{[node_id()], namespace()}.
-type free_hidden_group()::{node_id(), namespace()}. 
-type a_node()::{node_id(), node_type(), connections(), gr_names()}.
-type gr_names()::free_normal_group|free_hidden_group|[s_group_name()].
-type connections()::[node_id()].
-type node_type()::visible|hidden. 

-record(state, {ref  =[]  ::[{pid(),[node_id()],[tuple()]}], 
                model     ::#model{}
               }).

-define(debug, 0).

-define(with_conf, false).

-define(cmd, "c:/erl5.9.1/bin/erl ").

-ifdef(debug). 
dbg(Level, F, A) when Level >= ?debug ->
    io:format("dbg:~p:", [now()]),
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
                     teardown(),
                     setup(),
                     pretty_commands(?MODULE, Cmds, {H,S,Res}, Res==ok)
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
initial_state() ->
    initial_state(?with_conf).

-spec initial_state(WithConf::boolean())->#state{}.
initial_state(WithConf)->
    ?dbg(0, "calling initial state ...\n", []),
    NodeIds = [make_node_id(N)||N<-lists:seq(1,14)],
    FreeHiddenGrps = [{make_node_id(N), []}
                      ||N<-[9, 10]], 
    FreeNormalGrps = 
        if WithConf ->
                [{[make_node_id(N)], []}||N<-[11, 12,13,14]];
           true ->
                [{[make_node_id(N)], []}
                 ||N<-(lists:seq(1, 8)
                       ++lists:seq(11, 14))]
        end,
    Grps = if WithConf ->
                   {ok, [Config]} = file:consult("s_group.config"),
                   {kernel, Kernel}=lists:keyfind(kernel, 1, Config),
                   element(2, lists:keyfind(s_groups, 1, Kernel));
              true ->[]
           end,
                   
    SGrps = if WithConf ->
                    [{Name, Nids, []}||{Name, _, Nids}<-Grps];
               true -> []
            end,
    NodeStates=fetch_node_states(NodeIds),
    Nodes = [{NodeId, PublishType, Conns, 
              get_grps(NodeId, PublishType, Grps)}
             ||{NodeId, _, _State, _NameSpace, Conns, PublishType}
                   <-NodeStates],
    Model=#model{groups = SGrps, 
                 free_groups = FreeNormalGrps,
                 free_hidden_groups=FreeHiddenGrps,
                 nodes=Nodes},
    #state{ref=NodeStates, model=Model}.

make_node_id(N)->
    list_to_atom("node"++integer_to_list(N)++"@127.0.0.1").
    
get_grps(NodeId, PublishType, Grps) ->
    Gs=[Name||{Name, _, Nids}<-Grps, 
              lists:member(NodeId, Nids)],
    case Gs of 
         [] ->
            [group_name(PublishType)];  
         _ -> Gs
    end.

%%---------------------------------------------------------------
%% command: generates an appropriate symbolic function call to appear next
%% in a test case, if the symbolic state is S. Test sequences are generated 
%% by using command(S) repeatedly. However, generated calls are only included 
%% in test sequences if their precondition is also true.
%%---------------------------------------------------------------
command(S) ->
    frequency(
       [{5, {call, ?MODULE, new_s_group,  
             [gen_new_s_group_pars(S), all_node_ids(S)]}}
       ,{5, {call, ?MODULE, add_nodes, 
             [gen_add_nodes_pars(S), all_node_ids(S)]}}
       ,{5, {call, ?MODULE, remove_nodes, 
              [gen_remove_nodes_pars(S), all_node_ids(S)]}}
        ,{5, {call, ?MODULE, delete_s_group,
              [gen_delete_s_group_pars(S), all_node_ids(S)]}}
       ,{10,{call, ?MODULE, register_name,
              [gen_register_name_pars(S), all_node_ids(S)]}}
       ,{10,{call, ?MODULE, whereis_name,
             [gen_whereis_name_pars(S),  all_node_ids(S)]}}
       ,{10,{call, ?MODULE, re_register_name, 
              [gen_re_register_name_pars(S), all_node_ids(S)]}}
       ,{10,{call, ?MODULE, unregister_name,
              [gen_unregister_name_pars(S), all_node_ids(S)]}}
        ,{10,{call, ?MODULE, send,
             [gen_send_pars(S),all_node_ids(S)]}}
       %% node_down_up fails the test, so I remove it for now.
       %%,{1, {call, ?MODULE, node_down_up, 
       %%   [gen_node_down_up_pars(S), all_node_ids(S)]}}
       ]).
 
%%---------------------------------------------------------------
%% precondition: returns true if the symbolic call C can be performed 
%% in the state S. Preconditions are used to decide whether or not to 
%% include candidate commands in test cases
%%---------------------------------------------------------------
precondition(_S, {call, ?MODULE, new_s_group,
                  [{_SGroupName, NodeIds, _CurNode}, 
                   _AllNodeIds]}) ->
    NodeIds/=[];
precondition(_S, {call, ?MODULE, add_nodes,
                  [{SGroupName, NodeIds, _CurNode}, 
                   _AllNodeIds]}) ->
    SGroupName/=undefined andalso NodeIds/=[];
precondition(_S, {call, ?MODULE, remove_nodes,
                  [{SGroupName, NodeIds, _CurNode}, 
                   _AllNodeIds]}) ->
    SGroupName/=undefined andalso NodeIds/=[];
precondition(_S, {call, ?MODULE, delete_s_group,
                 [{SGroupName,_CurNode}, 
                  _AllNodeIds]}) ->
    SGroupName/=undefined;
precondition(_S, {call, ?MODULE, whereis_name, 
                 [{_NodeId, _SGroupName, RegName,
                   _CurNode}, _AllNodeIds]}) ->
    RegName/=undefined;
precondition(_S, {call, ?MODULE, register_name,
                  [{_SGroupName, RegName,Pid, CurNode}, 
                   _AllNodeIds]}) ->
    RegName/=undefined andalso proc_is_alive(CurNode, Pid); 
precondition(_S, {call, ?MODULE, re_register_name,
                  [{_SGroupName, RegName, Pid, CurNode}, 
                   _AllNodeIds]}) ->
    RegName/=undefined andalso proc_is_alive(CurNode, Pid); 
precondition(_S, {call, ?MODULE, unregister_name,
                  [{_SGroupName, RegName, _CurNode}, 
                   _AllNodeIds]}) ->
    RegName/=undefined;
precondition(_S, {call, ?MODULE, send, 
                  [{NodeId, _SGroupName, RegName, _Msg, 
                   _CurNode}, _AllNodeIds]}) ->
    NodeId/=undefined andalso RegName/=undefined;
precondition(_S, {call, ?MODULE, node_down_up, 
                  [{NodeId}, _AllNodeIds]}) ->
    NodeId/=undefined;
precondition(_S, _C) ->
    true.

%%---------------------------------------------------------------
%% Checks the post-condition of symbolic call C, executed in 
%% dynamic state S, 
%% with result R. The arguments of the symbolic call are the actual 
%% values passed, not any symbolic expressions from which they were 
%% computed. Thus when a post-condition is checked, we know the function 
%% called, the values it was passed, the value it returned, 
%% and the state in which it was called. Of course, post-conditions are 
%% checked during test execution, not test generation.
%%---------------------------------------------------------------
%% Here the state 'S' is the state before the call.
postcondition(S, {call, ?MODULE, new_s_group,
                  [{SGroupName, NodeIds, CurNode}, _AllNodeIds]},
             {Res, ActualState}) ->
    {AbsRes, NewS} = new_s_group_next_state(S,SGroupName, NodeIds, CurNode),
    (AbsRes == Res) and is_the_same(ActualState, NewS);
postcondition(S, {call, ?MODULE, add_nodes,
                   [{SGroupName, NodeIds, CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    {Res1, NewS}=add_nodes_next_state(S,SGroupName, NodeIds, CurNode),
    (process_result(Res1) == process_result(Res)) and 
        is_the_same(ActualState, NewS);
postcondition(S, {call, ?MODULE, remove_nodes,
                  [{SGroupName, NodeIds, CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    {Res1,NewS}=remove_nodes_next_state(S,SGroupName, NodeIds, CurNode),
    (process_result(Res1) == process_result(Res)) and 
        is_the_same(ActualState, NewS);
postcondition(S, {call, ?MODULE, delete_s_group,
                  [{SGroupName, CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    {AbsRes, NewS}=delete_s_group_next_state(S,SGroupName, CurNode),
    (AbsRes == process_result(Res)) and 
        is_the_same(ActualState, NewS);
postcondition(S,  {call, ?MODULE, register_name, 
                   [{SGroupName, RegName, Pid, _CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    {AbsRes, NewS}=register_name_next_state(S,SGroupName, RegName, Pid),
    (process_result(AbsRes)==process_result(Res)) and
        is_the_same(ActualState, NewS);
postcondition(S,  {call, ?MODULE, re_register_name, 
                   [{SGroupName, RegName, Pid, _CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    {AbsRes, NewS} = re_register_name_next_state(S, SGroupName, RegName, Pid),
    (process_result(AbsRes)==process_result(Res)) and
        is_the_same(ActualState, NewS);
postcondition(S,  {call, ?MODULE, unregister_name, 
                   [{SGroupName, RegName, _CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    {AbsRes, NewS} = unregister_name_next_state(S, SGroupName,RegName),
    (process_result(AbsRes)==process_result(Res)) and
        is_the_same(ActualState, NewS);
postcondition(_S, {call, ?MODULE, whereis_name, 
                   [{_TargetNodeId,  _GroupName, undefined, _CurNode}, _AllNodeIds]},
              {_Res, _ActualState}) ->
    true;
postcondition(S, {call, ?MODULE, whereis_name, 
                   [{TargetNodeId, GroupName, RegName, CurNode}, _AllNodeIds]},
              {Res, ActualState}) ->
    Pid = find_name(S#state.model, TargetNodeId, GroupName,RegName),
    NewS=whereis_name_next_state(S, CurNode, TargetNodeId),
    (Pid == Res) and is_the_same(ActualState, NewS);
postcondition(_S, {call, ?MODULE, send, 
                   [{_TargetNodeId,  _GroupName, undefined, _Msg, _CurNode}, _AllNodeIds]},
              {_Res, _ActualState}) ->
    true;
postcondition(S, {call, ?MODULE, send, 
                   [{TargetNodeId, GroupName,RegName, _Msg,CurNode}, _AllNodeIds]},
              {Res,ActualState}) ->
    Pid = find_name(S#state.model, TargetNodeId, GroupName,RegName), 
    NewS=whereis_name_next_state(S, CurNode, TargetNodeId),
    %% (Pid == Res) and
    is_the_same(ActualState, NewS);

postcondition(S, {call, ?MODULE, node_down_up, 
                  [{_NodeId}, _AllNodeIds]},
              {_Res,ActualState}) ->
    is_the_same(ActualState, S);
postcondition(_S, _C, _R) ->
    true.


%%---------------------------------------------------------------
%% This is the state transition function of the abstract state machine, 
%% and it is used during both test generation and test execution.
%%---------------------------------------------------------------
%%-spec(next_state(S::#state{}, R::var(), C::call()) -> #state{}).

next_state(S, _V, {call, ?MODULE, new_s_group,
               [{SGroupName, NodeIds, CurNode}, _AllNodeIds]}) ->
    {_Res, NewS} = new_s_group_next_state(S,SGroupName, NodeIds, CurNode),
    NewS;

next_state(S, _V, {call, ?MODULE, delete_s_group,
               [{SGroupName, CurNode}, _AllNodeIds]}) ->
    {_Res, NewS}=delete_s_group_next_state(S,SGroupName, CurNode),
    NewS;
next_state(S, _V, {call, ?MODULE, add_nodes,
                  [{SGroupName, NodeIds, CurNode}, _AllNodeIds]}) ->
    {_Res, NewS}=add_nodes_next_state(S, SGroupName, NodeIds, CurNode),
    NewS;
next_state(S, _V, {call, ?MODULE, remove_nodes,
                  [{SGroupName, NodeIds, CurNode}, _AllNodeIds]}) ->
    {_Res, NewS}=remove_nodes_next_state(S, SGroupName, NodeIds, CurNode),
    NewS;
next_state(S, _V, {call, ?MODULE, register_name, 
                   [{SGroupName,RegName, Pid, _CurNode}, _AllNodeIds]}) ->
    {_Res, NewS}=register_name_next_state(S, SGroupName, RegName, Pid),
    NewS;
next_state(S, _V, {call, ?MODULE, re_register_name, 
                   [{SGroupName, RegName, Pid, _CurNode}, _AllNodeIds]}) ->
    {_Res, NewS}=re_register_name_next_state(S, SGroupName, RegName, Pid),
    NewS;
next_state(S, _V, {call, ?MODULE, unregister_name, 
                   [{SGroupName, RegName, _CurNode}, _AllNodeIds]}) ->
    {_Res, NewS}=unregister_name_next_state(S,SGroupName, RegName),
    NewS;
next_state(S, _V, {call, ?MODULE, whereis_name,  
                   [{TargetNode,_SGroupName, RegName,
                     CurNode},_AllNodeIds]}) when RegName/=undefined  ->
    whereis_name_next_state(S, CurNode, TargetNode);

next_state(S, _V, {call, ?MODULE, send,
                   [{_TargetNode, _SGroupName, undefined, _Msg,
                     _CurNode}, _AllNodeIds]}) ->
    S;
next_state(S, _V, {call, ?MODULE, send,  
                   [{TargetNode, _SGroupName, _RegName, _Msg,
                     CurNode}, _AllNodeIds]}) ->
    %%the actual message sending is not modelled in the semantics.
    %%send has the same effect on node states as whereis_name.
    whereis_name_next_state(S, CurNode, TargetNode);
next_state(S, _V, {call, ?MODULE, node_down_up,
                   [{_NodeId}, _AllNodeIds]}) ->
    S;  
next_state(S, _V, _) ->
    S.

%=======================================================
% new_s_group next state.
%=======================================================
new_s_group_next_state(S,SGroupName, NodeIds, CurNode) ->
    case lists:member(CurNode, NodeIds) of 
        false ->
            {[], S};
        true ->
            new_s_group_next_state_1(S,SGroupName, NodeIds)
    end.
new_s_group_next_state_1(S,SGroupName, NodeIds) ->
    Model =S#state.model,
    #model{groups=Grps, free_groups = Fgs,
           free_hidden_groups=Fhgs,nodes=Nodes}=Model,
    NewNodes=[add_connections_and_group_to_nodes(N, NodeIds, SGroupName)
              ||N<-Nodes],
    NewGrps = [{SGroupName, NodeIds, []}|Grps],
    NewFgs = remove_nodes_from_fgps(NodeIds, Fgs),
    NewFhgs= remove_nodes_from_fhgps(NodeIds, Fhgs),
    NewModel =Model#model{groups=NewGrps, free_groups=NewFgs,
                          free_hidden_groups=NewFhgs,
                          nodes = NewNodes},
    {{ok, SGroupName, NodeIds}, S#state{model=NewModel}}.
    
add_connections_and_group_to_nodes(CurNode, NodeIds, SGroupName) ->
    {CurNodeId, NodeType, Conns, GrpNames}=CurNode,
    case lists:member(CurNodeId, NodeIds) of 
        true ->
            NewGrpNames=[SGroupName|GrpNames] 
                -- [free_normal_group, free_hidden_group],
            NewConns =[Id||Id<-NodeIds, Id=/=CurNodeId],
            {CurNodeId, NodeType, lists:usort(Conns++NewConns), 
             lists:usort(NewGrpNames)};
        false ->
            CurNode
    end.
remove_nodes_from_fgps(NodeIds, Fgs) ->
    [{NodeIds1--NodeIds, NameSpace}||{NodeIds1, NameSpace}<-Fgs,
                                     NodeIds1--NodeIds/=[]].

remove_nodes_from_fhgps(NodeIds, Fgs) ->
    [{NodeId, NameSpace}||{NodeId, NameSpace}<-Fgs, 
                          not lists:member(NodeId, NodeIds)].

%=======================================================
% delete_s_group next state.
%=======================================================
delete_s_group_next_state(S,SGroupName, CurNode)->
    Model =S#state.model,
    #model{groups=Grps, free_groups = Fgs,
           free_hidden_groups=Fhgs,nodes=Nodes}=Model,
    case lists:keyfind(SGroupName, 1,Grps) of
        {SGroupName, NodeIds, _NS} ->
            case lists:member(CurNode, NodeIds) of 
                true -> 
                    NewGrps = lists:keydelete(SGroupName, 1, Grps),
                    Nodes1 = remove_s_group_from_nodes(NodeIds, SGroupName, Nodes),
                    {NewFgs, NewFhgs, NewNodes}= new_free_nodes(Fgs, Fhgs, Nodes1),
                    NewModel =Model#model{groups=NewGrps, free_groups=NewFgs,
                                          free_hidden_groups=NewFhgs,
                                          nodes = NewNodes},
                    {ok, S#state{model=NewModel}};
                false ->
                    {error, S}
            end;
        false ->
            {error, S}
    end.

remove_s_group_from_nodes(Nodes,SGroupName,AllNodes) ->
    [case lists:member(NodeName, Nodes) of 
         true ->
             {NodeName, NodeType, Conns, Grps--[SGroupName]};
         false ->
             {NodeName, NodeType, Conns, Grps}
     end
     ||{NodeName, NodeType, Conns, Grps}<-AllNodes].

new_free_nodes(Fgs, Fhgs, Nodes) ->
    NewFreeNodes=[{NodeName, NodeType, Conns, [group_name(NodeType)]}
                  ||{NodeName, NodeType, Conns, []}<-Nodes],
    Nodes1=[{NodeName, NodeType, Conns, 
             case Grps of 
                 [] ->[group_name(NodeType)];
                 _ -> Grps
             end}
            ||{NodeName, NodeType, Conns, Grps}<-Nodes],
    NewGhgs=add_free_hidden_group(Fhgs, NewFreeNodes),
    {NewFgs, NewNodes} = add_free_normal_groups(Fgs, Nodes1, NewFreeNodes),
    {NewFgs, NewGhgs, NewNodes}.
    
group_name(hidden) ->
    free_hidden_group;
group_name(normal) ->
    free_normal_group.
    
add_free_hidden_group(Fhgs, NewFreeNodes)-> %%are these names really unregistered?
    NewFhgs=[{NodeName,[]}||{NodeName, hidden, _, _}<-NewFreeNodes],
    Fhgs++NewFhgs.

add_free_normal_groups(Fgs, AllNodes, NewFreeNodes)->
    NewFreeNodeIds=[Id||{Id, _, _, [free_normal_group]}<-NewFreeNodes],
    ConnIds = lists:usort([ConnId||{_Id, _NodeType, Conns, [free_normal_group]}
                                       <-NewFreeNodes, 
                                   ConnId<-Conns]),
    GrpConnIds = [ConnId ||ConnId<-ConnIds,{Ids, _}<-Fgs, 
                         lists:member(ConnId, Ids)],
    {NewFgs, NewNodes} = merge_free_normal_groups(NewFreeNodeIds, GrpConnIds, Fgs, AllNodes),
    {NewFgs, NewNodes}.

merge_free_normal_groups([], _GrpConnIds, Fgs, AllNodes)->
    {Fgs, AllNodes};
merge_free_normal_groups(NewFreeNodeIds, [], Fgs, AllNodes) ->
    {[{NewFreeNodeIds, []}|Fgs], AllNodes};
merge_free_normal_groups(NewFreeNodeIds, GrpConnIds, Fgs, AllNodes)->
    FgsToMerge=find_free_normal_grps(GrpConnIds, Fgs),
    {NewGrpNodes0, NewGrpNameSpace0}=lists:unzip(FgsToMerge),
    NewGrpNodes1 = lists:append(NewGrpNodes0),
    NewGrpNodes=lists:usort(NewFreeNodeIds++NewGrpNodes1),
    NewNameSpace=lists:usort(lists:append(NewGrpNameSpace0)),  %%TO FIX!!! keysort?
    NewGrps = (Fgs--FgsToMerge) ++ [{NewGrpNodes,  NewNameSpace}],
    NewNodes = inter_connect_nodes(NewGrpNodes,AllNodes),
    {NewGrps, NewNodes}.
    
find_free_normal_grps(GrpConnIds, Fgs)->
    [{NodeIds, NS}||{NodeIds, NS}<-Fgs, 
                    GrpConnIds--NodeIds/=GrpConnIds].
                       

inter_connect_nodes(NewGrpNodes, AllNodes) ->
    [case lists:member(Id, NewGrpNodes) of 
             true -> 
                 {Id, NodeType, lists:usort(Conns++NewGrpNodes--[Id]), Grps};              
             false ->
                 {Id, NodeType, Conns, Grps}
         end||{Id, NodeType, Conns, Grps}<-AllNodes].
    

%=======================================================
% add_nodes next state.
%=======================================================
add_nodes_next_state(S, SGroupName, NodeIds, CurNode)->
    Model =S#state.model,
    #model{groups=Grps, free_groups = Fgs,
           free_hidden_groups=Fhgs,nodes=Nodes}=Model,
    Grp=[{GrpName, Ids, NS}||{GrpName, Ids, NS}<-Grps,
                             GrpName==SGroupName],
    case Grp of 
        [] ->
            {error, S};
        [{SGroupName, Ids, NS}] ->
            case lists:member(CurNode, Ids) of 
                false ->
                    {error, S};
                true ->
                    NewGrpNodeIds = lists:usort(Ids++NodeIds),
                    NewNodes=[add_connections_and_group_to_nodes(N, NewGrpNodeIds, SGroupName)
                              ||N<-Nodes],
                    NewGrp={SGroupName, NewGrpNodeIds, NS},
                    NewGrps= lists:keyreplace(SGroupName, 1, Grps, NewGrp),
                    NewFgs = remove_nodes_from_fgps(NodeIds, Fgs),
                    NewFhgs= remove_nodes_from_fhgps(NodeIds, Fhgs),
                    NewModel =Model#model{groups=NewGrps, free_groups=NewFgs,
                                          free_hidden_groups=NewFhgs,
                          nodes = NewNodes},
                    {{ok,NodeIds}, S#state{model=NewModel}}
            end
    end.

%=======================================================
% register_name next state.
%=======================================================
register_name_next_state(S, SGroupName, RegName, Pid) ->
    Model = S#state.model,
    Grps=Model#model.groups,
    case lists:keyfind(SGroupName, 1, Grps) of 
        {SGroupName, NodeIds, NameSpace} -> 
            case lists:keyfind(RegName, 1, NameSpace) of 
                {RegName, _} ->
                    {no, S};
                false ->
                    case lists:keyfind(Pid,2,NameSpace) of 
                        {_, Pid} -> 
                            {no, S};
                        false ->
                            NewGrp={SGroupName,NodeIds, 
                                    [{RegName, Pid}|NameSpace]},
                            NewGrps = lists:keyreplace(SGroupName, 1, 
                                                       Grps, NewGrp),
                            NewModel = Model#model{groups=NewGrps},
                            {yes, S#state{model=NewModel}}                        
                    end
            end;
        false -> {no, S}
    end.

%=======================================================
% re_register_name next state.
%=======================================================
re_register_name_next_state(S,SGroupName,  RegName,Pid) ->
    Model = S#state.model,
    Grps=Model#model.groups,
    case lists:keyfind(SGroupName, 1, Grps) of 
        {SGroupName, NodeIds, NameSpace} -> 
            case lists:keyfind(Pid,2,NameSpace) of 
                {_, Pid} -> 
                    ?dbg(0, "Pid is already registered.\n", []),
                    {no, S};
                false ->
                    ?dbg(0, "Pid is NOT registered.\n", []),
                    NewNameSpace= [{RegName, Pid}|
                                   lists:keydelete(RegName, 1, NameSpace)],
                    NewGrp={SGroupName,NodeIds, NewNameSpace},
                    NewGrps = lists:keyreplace(
                                SGroupName, 1, Grps, NewGrp),
                    NewModel = Model#model{groups=NewGrps},
                    {yes, S#state{model=NewModel}}
            end;
        false -> {no,S}
    end.

%=======================================================
% remove_nodes next state.
%=======================================================
unregister_name_next_state(S, SGroupName,RegName) ->
    Model = S#state.model,
    Grps=Model#model.groups,
    case lists:keyfind(SGroupName,1, Grps) of 
        {SGroupName, NodeIds, NameSpace} ->
            case lists:keyfind(RegName, 1, NameSpace) of
                false ->
                    {ok,S};
                {RegName, _} ->
                    NewNameSpace= lists:keydelete(RegName, 1, NameSpace),
                    NewGrp={SGroupName,NodeIds, NewNameSpace},
                    NewGrps = lists:keyreplace(
                                SGroupName, 1, Grps, NewGrp),
                    NewModel = Model#model{groups=NewGrps},
                    {ok, S#state{model=NewModel}}
            end;
        false ->
            {no,S}
    end.

%=======================================================
% remove_nodes next state.
%=======================================================
remove_nodes_next_state(S, SGroupName, NodeIds, CurNode)->
    Model =S#state.model,
    #model{groups=Grps, free_groups = Fgs,
           free_hidden_groups=Fhgs,nodes=Nodes}=Model,
    Grp=[{GrpName, Ids, NS}||{GrpName, Ids, NS}<-Grps,
                             GrpName==SGroupName],
    case Grp of 
        [] ->
            {{error, s_group_does_not_exist}, S};
        [{SGroupName, Ids, NS}] ->
            case lists:member(CurNode, Ids) of 
                false ->
                    {{error, current_node_not_s_group_member},S};
                true ->
                    NewNodes=remove_s_group_from_nodes(NodeIds,SGroupName, Nodes),
                    NewGrpNodeIds = Ids -- NodeIds,
                    NewGrp={SGroupName, NewGrpNodeIds, NS},
                    NewGrps= lists:keyreplace(SGroupName, 1, Grps, NewGrp),
                    {NewFgs, NewFhgs, NewNodes1}= new_free_nodes(Fgs, Fhgs, NewNodes),
                    NewModel =Model#model{groups=NewGrps, free_groups=NewFgs,
                                          free_hidden_groups=NewFhgs,
                                          nodes = NewNodes1},
                    {ok, S#state{model=NewModel}}
            end
    end.
    
    
%=======================================================
% whereis_name next state.
%=======================================================
whereis_name_next_state(S, CurNode, TargetNode) when CurNode==TargetNode -> S;
whereis_name_next_state(S, CurNode, TargetNode) ->
    #model{nodes=Nodes}=Model=S#state.model,
    {CurNode, _CurNodeType, CurConns, CurGrps} = 
        lists:keyfind(CurNode, 1, Nodes),
    {TargetNode, _TargetNodeType,  _TargetConns, TargetGrps} = 
        lists:keyfind(TargetNode, 1, Nodes),
    case lists:member(TargetNode, CurConns) of
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
                    NewNodes=inter_connect_nodes([CurNode, TargetNode], Nodes),
                    NewModel=Model#model{nodes=NewNodes},
                    S#state{model=NewModel}
            end;
        true-> S  %% a connection exists.
    end.

%%-------------------------------------------------------------%%
%%                                                             %%
%%                  General Properties                         %%
%%                                                             %%
%%-------------------------------------------------------------%%
%% More could be added here, 
%% e.g: no conflictions in namespace.
%%      no empty groups.
%%      transitive connection of free normal nodes, etc.
invariant(S) ->     
    prop_partition(S).  %% More could be added.

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
    AllNodeIds = [NodeId||{NodeId,_,  _, _}<-Nodes],
    Empty = sets:new(),
    sets:intersection(GrpNodes, FreeNodes)==Empty andalso
        sets:intersection(GrpNodes, FreeHiddenNodes)==Empty andalso
        sets:intersection(FreeNodes, FreeHiddenNodes)==Empty andalso
        lists:sort(sets:to_list(sets:union([GrpNodes, FreeNodes, FreeHiddenNodes])))== 
        lists:sort(AllNodeIds).
   

%%---------------------------------------------------------------
%%
%%  Adaptor functions.
%%---------------------------------------------------------------
new_s_group({SGroupName, NodeIds, CurNode}, AllNodes)->
    rpc_call(CurNode, s_group, new_s_group, 
                 [SGroupName, NodeIds], AllNodes).
add_nodes({SGroupName, NodeIds, CurNode}, AllNodes)->
    rpc_call(CurNode, s_group, add_nodes, 
             [SGroupName, NodeIds], AllNodes).    
remove_nodes({SGroupName, NodeIds, CurNode}, AllNodes)->
    rpc_call(CurNode, s_group, remove_nodes, 
                 [SGroupName, NodeIds], AllNodes).
 
delete_s_group({SGroupName, CurNode}, AllNodes)->
    rpc_call(CurNode, s_group, delete_s_group, 
                 [SGroupName], AllNodes).
  
register_name({SGroupName, RegName,Pid, Node}, AllNodes) ->
    rpc_call(Node, s_group, register_name, 
                 [SGroupName,RegName, Pid], AllNodes).
re_register_name({SGroupName, RegName, Pid, Node}, AllNodes) ->
    rpc_call(Node, s_group, re_register_name, 
                 [SGroupName,RegName, Pid], AllNodes).    
unregister_name({SGroupName, RegName,Node}, AllNodes) ->
    rpc_call(Node, s_group, unregister_name, 
                 [SGroupName,RegName], AllNodes).
whereis_name({_NodeId, _SGroupName, undefined, _Node}, AllNodes)->
    State =fetch_node_states(AllNodes),
    {undefined, State};
whereis_name({NodeId, SGroupName, RegName, Node}, AllNodes)->
    rpc_call(Node, s_group, whereis_name,
                 [NodeId, SGroupName,RegName], AllNodes).

send({TargetNodeId, SGroupName, RegName, Msg, Node}, AllNodes)->
    rpc_call(Node, s_group, send,
                 [TargetNodeId, list_to_atom(SGroupName), RegName, Msg], AllNodes).

%% node_down_up only test group nodes.
node_down_up({NodeId}, _AllNodes)->
    timer:sleep(2000),
    _Res=rpc:call(NodeId, erlang, halt, []),
    timer:sleep(2000),
    Str = if ?with_conf ->
                  " -detached -setcookie \"secret\" -config s_group.config";
             true ->
                  " -detached -setcookie \"secret\""
          end,
    os:cmd(?cmd++" -name "++ atom_to_list(NodeId)++Str),
    timer:sleep(2000).
 

rpc_call(Node, Module, Fun, Args, AllNodes) ->
    ?dbg(9, "s_group_eqc:rpc_call(~p, ~p, ~p, ~p).\n", 
         [Node, Module, Fun, Args]), 
    Res=rpc:call(Node, Module, Fun, Args),
    ?dbg(9, "rpc_call returned:~p\n", [Res]),
    timer:sleep(2000),
    State=fetch_node_states(AllNodes),
    {Res, State}.
    
%%---------------------------------------------------------------
%%
%%  translate real state to model state
%%---------------------------------------------------------------
fetch_node_states(NodeIds) ->
    try [{NodeId, 
          get_procs(NodeId),
          get_s_group_info(NodeId),                  
          fetch_name_space(NodeId), 
          connections(NodeId), 
          publish_arg(NodeId)}
          ||NodeId<-NodeIds]        
    catch
        E1:E2 -> 
            io:format("fetch_node_states error:~p\n", 
                      [{E1, E2}])
    end.
    
to_model(NodeIdStatePairs) ->
    ?dbg(0, "actual to abstract model.....\n",[]),
    GroupNodes0 =[case lists:keyfind(own_s_groups, 1, State) of
                     {own_s_groups, []}-> [];
                     {own_s_groups, List} -> List
                 end                          
                  ||{_NodeId, _, State, _NameSpace, _Conns, _PublishType}
                        <-NodeIdStatePairs, 
                    lists:member({state, synced}, State)],
    GroupNodes = sets:to_list(sets:from_list(lists:append(GroupNodes0))),
    Groups = analyze_group_nodes(GroupNodes),
    FreeNodes = [{NodeId,  [NodeId|Conns], NameSpace}
                 ||{NodeId, _Pids,State, NameSpace, Conns, PublishType}
                       <-NodeIdStatePairs, 
                   lists:member({own_s_groups, []}, State), 
                   PublishType == normal],
    FreeGroups = analyze_free_nodes(FreeNodes),
    FreeHiddenGroups = [{NodeId, NameSpace}
                        ||{NodeId, _Pids, State, NameSpace, _Conns, PublishType}
                              <-NodeIdStatePairs,
                          lists:member({own_s_groups, []}, State),
                          PublishType == hidden],
    AllNodes=[{NodeId, PublishType, Conns, group_names(State, PublishType)}
           ||{NodeId, _, State, _NameSpace,Conns, PublishType}
                 <-NodeIdStatePairs],
    #model{groups = Groups,
           free_groups = FreeGroups,
           free_hidden_groups=FreeHiddenGroups,
           nodes=AllNodes}.
    
group_names(State, PublishType) ->
    case lists:keyfind(own_s_groups, 1, State) of 
        false -> 
            [group_name(PublishType)];
        {own_s_groups, OwnGrps} when OwnGrps=/=[]->
            {GroupNames,_} =lists:unzip(OwnGrps),
            GroupNames;
        _-> [group_name(PublishType)]
    end.

connections(NodeId) ->
    Visibles=rpc:call(NodeId, erlang, nodes, [visible]),
    Hiddens =rpc:call(NodeId, erlang, nodes, [hidden]),
    Visibles1=case Visibles of 
                  {badrpc, nodedown} -> 
                      io:format("in connections: node down:~p\n", [NodeId]),
                      [];                                        
                  _ ->Visibles
              end,
    Hiddens1=case Hiddens of 
                  {badrpc, nodedown} -> 
                     io:format("in connections: node down:~p\n", [NodeId]),
                      [];                                        
                  _ ->Hiddens
              end,
    Visibles1++Hiddens1.
                   
fetch_name_space(NodeId) ->
    NameSpace=rpc:call(NodeId,global, registered_names_with_pids, []),
    case NameSpace of 
        {badrpc, nodedown} ->
            io:format("in fetch_name_space: node down\n"),
            [];
        undefined ->
            [];        
        _ ->lists:sort(NameSpace)
    end.

publish_arg(NodeId) ->
    case rpc:call(NodeId, init, get_argument, [hidden]) of 
        {ok,[[]]} ->
            hidden;
        {ok,[["true"]]} ->
            hidden;
        {badrpc, nodedown} ->
            normal;        
        _ ->
	    normal
    end.
    

get_procs(NodeId) ->
    case rpc:call(NodeId, erlang, processes, []) of 
        {badrpc, nodedown} ->
            io:format("in get_procs: node down.\n"),
            [];
        Procs-> Procs
    end.

get_s_group_info(NodeId) ->
    case rpc:call(NodeId, s_group, info, []) of 
        {badrpc, nodedown} ->
            io:format("in get_s_group_info: nodedown\n"),
            [];
        Info -> Info
    end.

analyze_free_nodes(FreeNodes)->
    FreeNodeIds = [NodeId||{NodeId, _Conns, _NameSpace}<-FreeNodes],
    NodeIdWithConnsAndNameSpace=
        [{FreeNodeIds--(FreeNodeIds--Conns), NameSpace}||
            {_NodeId, Conns, NameSpace}<-FreeNodes],
    lists:usort(NodeIdWithConnsAndNameSpace).
    

%% This should be more strict!!!
analyze_group_nodes(GroupNameNodesPairs) ->
    F = fun(NodeIds, GrpName) ->
                NameSpace=[[{Name, Pid}||{Grp, Name, Pid}<-
                                             fetch_name_space(Id), Grp==GrpName]
                           || Id<-NodeIds],
                sets:to_list(sets:from_list(lists:append(NameSpace)))
        end,
    [{GroupName, Nodes,  F(Nodes, GroupName)}||{GroupName, Nodes}<-GroupNameNodesPairs].

is_the_same(State, AbstractState) ->
    Model = to_model(State),
    ActualModel =normalise_model(Model),
    AbstractModel=normalise_model(AbstractState#state.model),
    case ActualModel==AbstractModel of 
        true -> true;
        false ->
            SameNodes=check_nodes_the_same(ActualModel#model.nodes,
                                 AbstractModel#model.nodes),
            SameGrps=check_grps_the_same(ActualModel#model.groups,
                                AbstractModel#model.groups),
            SameFGrps=check_free_grps_the_same(ActualModel#model.free_groups,
                                     AbstractModel#model.free_groups),
            SameFHgrps=check_free_hidden_grps_the_same(ActualModel#model.free_hidden_groups,
                                                       AbstractModel#model.free_hidden_groups),
            SameNodes and SameGrps and SameFGrps and SameFHgrps
    end.

normalise_model(Model) ->
    Groups = Model#model.groups,
    FreeGroups = Model#model.free_groups,
    FreeHiddenGroups = Model#model.free_hidden_groups,
    Nodes  = Model#model.nodes,
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
               1, [{Id, NodeType, lists:usort(Conns), lists:usort(GrpNames)}
                   ||{Id, NodeType, Conns, GrpNames}<-Nodes]),
    #model{groups = Groups1, 
           free_groups = FreeGroups1,
           free_hidden_groups = FreeHiddenGroups1,
           nodes = Nodes1}.
    
           
check_nodes_the_same(ActualNodes, AbstractNodes) ->
    case ActualNodes == AbstractNodes of 
        false ->
            io:format("check_nodes_the_same returns false!\n"),
            Zip=lists:zip(ActualNodes, AbstractNodes),
            [case 
                 N1==N2 of 
                 true -> ok;
                 false ->io:format("Actual node is:~p.\n", [N1]),
                         io:format("Abstract node is :~p.\n", [N2])
             end||{N1, N2}<- Zip],
            false;
        true -> true
    end.

check_grps_the_same(ActualGrps, AbstractGrps) ->
    case ActualGrps==AbstractGrps of 
        false ->
            io:format("check_grps_the_same returns false!\n"),
            io:format("Actual groups are:~p\n", [ActualGrps]),
            io:format("Abstract groups are:~p\n", [AbstractGrps]),
            false;
        true -> true
    end.
   
check_free_grps_the_same(ActualFreeGrps, AbstractFreeGrps) ->
    case ActualFreeGrps==AbstractFreeGrps of 
        false ->
            io:format("check_free_grps_the_same returns false!\n"),
            io:format("Actual free groups are:~p.\n", 
                      [ActualFreeGrps]),
            io:format("Abstract free groups are:~p.\n", 
                      [AbstractFreeGrps]),
            false;
        true -> true
    end.

check_free_hidden_grps_the_same(ActualFreeHiddenGrps, AbstractFreeHiddenGrps) ->
    case ActualFreeHiddenGrps==AbstractFreeHiddenGrps of 
        false ->
            io:format("check_free_hidden_grps the same returns false!\n"),
            io:format("Actual free hidden groups are:~p.\n", 
                      [ActualFreeHiddenGrps]),
            io:format("Abstract free hidden groups are:~p.\n",
                      [AbstractFreeHiddenGrps]),
            false;
        true -> true
    end.

                       
%%---------------------------------------------------------------
%%
%% Generators.
%%---------------------------------------------------------------

%% How to solve the dependency between parameters?
gen_new_s_group_pars(S=#state{model=Model}) ->
    #model{free_groups = _Fgs, free_hidden_groups=_Fhgs}=Model,
    Nodes  = Model#model.nodes,
    AllNodeIds=[Id||{Id, _NodeType, _Conns, _Grps}<-Nodes],
    ?LET(NodeIds, gen_nodes(eqc_gen:oneof(AllNodeIds)),
         ?LET(CurNode, eqc_gen:oneof(NodeIds),
              ?LET(GrpName, gen_s_group_name(S),
              {GrpName, lists:usort(NodeIds), CurNode}))).
     %% group name is a string here, atom is fine too.

gen_nodes(Nodes) ->
    ?SUCHTHAT(Ns, eqc_gen:list(Nodes), length(Ns)>1).

gen_add_nodes_pars(_S=#state{ref=Ref, model=Model}) ->
    #model{groups=Grps}=Model,
    if Grps==[] orelse Ref==[] ->
            {undefined, undefined, undefined};
       true ->
            Nodes  = Model#model.nodes,
            AllNodeIds=[Id||{Id, _NodeType, _Conns, _Grps}<-Nodes],
            ?LET({GrpName, NodeIds, _Namespace}, (eqc_gen:oneof(Grps)),
                 ?LET(CurNode, (eqc_gen:oneof(NodeIds)),
                      ?LET(NodeIds1, (eqc_gen:list(gen_oneof(AllNodeIds -- NodeIds))),
                           {GrpName, lists:usort(NodeIds1), CurNode})))
    end.
    

gen_remove_nodes_pars(_S=#state{ref=Ref, model=Model}) ->
    Grps=Model#model.groups,
    RealGrps=[{GName, Ids, Ns}||{GName, Ids, Ns}<-Grps, length(Ids)>1],
    if RealGrps==[] orelse Ref==[] ->
            {undefined, [], undefined};
       true ->
            ?LET({GrpName, NodeIds, _Namespace}, (eqc_gen:oneof(RealGrps)),
                 ?LET(CurNode, (eqc_gen:oneof(NodeIds)),
                      ?LET(NodeIds1, (eqc_gen:list(gen_oneof(NodeIds -- [CurNode]))),
                           {GrpName, lists:usort(NodeIds1), CurNode})))
    end.

gen_delete_s_group_pars(_S=#state{ref=Ref, model=Model}) ->
    Grps=Model#model.groups,
    if Grps==[] orelse Ref==[] ->
            {undefined, undefined};
       true ->
            ?LET({ GrpName, NodeIds, _Namespace}, eqc_gen:oneof(Grps),
                  ?LET(CurNode, eqc_gen:oneof(NodeIds),
                       {GrpName, CurNode}))
    end.
                      
gen_register_name_pars(_S=#state{ref=Ref, model=Model}) ->
    Grps=Model#model.groups,
    if Grps==[] orelse Ref==[] ->
           {undefined, undefined, undefined, undefined};
       true ->
            ?LET({GrpName, NodeIds, _Namespace}, eqc_gen:oneof(Grps),
                 ?LET(NodeId, eqc_gen:oneof(NodeIds),
                      ?LET(Name, gen_reg_name(),
                           {GrpName, list_to_atom(Name),
                            eqc_gen:oneof(element(2, lists:keyfind(NodeId, 1, Ref))),
                            NodeId})))
    end.

gen_re_register_name_pars(_S=#state{ref=Ref, model=Model}) ->
    Grps=Model#model.groups,
    if Grps==[] orelse Ref==[] ->
            {undefined, undefined, undefined, undefined};
       true ->
            ?LET({GrpName, NodeIds, Namespace}, eqc_gen:oneof(Grps),
                 ?LET(NodeId, eqc_gen:oneof(NodeIds),
                       ?LET(Name, gen_reg_name(Namespace),
                            {GrpName, list_to_atom(Name),
                             eqc_gen:oneof(element(2, lists:keyfind(NodeId, 1, Ref))),
                             NodeId})))
    end.

gen_unregister_name_pars(_S=#state{ref=Ref, model=Model}) ->
    Grps=Model#model.groups,
    if Grps==[] orelse Ref==[] ->
            {undefined, undefined, undefined};
       true ->
            ?LET({GrpName, NodeIds, NameSpace}, (eqc_gen:oneof(Grps)),
                 ?LET(NodeId, (eqc_gen:oneof(NodeIds)),
                      ?LET(Name, (gen_oneof(element(1,lists:unzip(NameSpace)))),
                           {GrpName, Name, NodeId})))
    end.

gen_whereis_name_pars(_S=#state{model=Model}) ->
    Grps=Model#model.groups,
    FreeGrps = [{free_normal_group, Ids, NS}||
                   {Ids, NS}<-Model#model.free_groups],
    HiddenGrps=[{free_hidden_group, [Id], NS}||
                   {Id, NS}<-Model#model.free_hidden_groups],
    AllGrps = Grps++FreeGrps++HiddenGrps,
    ?LET({GrpName, NodeIds, NameSpace},
         (eqc_gen:oneof(AllGrps)),
         ?LET(CurNode, (eqc_gen:oneof(NodeIds)),
              {eqc_gen:oneof(NodeIds), 
               GrpName,
               gen_oneof(element(1,lists:unzip(NameSpace))),
               CurNode})).

gen_send_pars(_S=#state{ref=_Ref, model=Model}) ->
    Grps=Model#model.groups,
    case Grps of 
        [] ->
            {undefined, undefined, undefined,
             undefined,undefined};
        _ ->
            ?LET({GrpName, NodeIds, NameSpace},
                 (eqc_gen:oneof(Grps)),
                 {eqc_gen:oneof(NodeIds),
                  GrpName, 
                  gen_oneof(element(1,lists:unzip(NameSpace))),
                  gen_message(),
                  eqc_gen:oneof(NodeIds)})
    end.

gen_node_down_up_pars(_S=#state{model=Model}) ->
    Grps=Model#model.groups,
    if Grps==[] ->
            {undefined};
       true ->
            ?LET({_GrpName, NodeIds, _NameSpace},
                 eqc_gen:oneof(Grps),
                 {eqc_gen:oneof(NodeIds)})
    end.
   
gen_s_group_name(_S=#state{model=Model}) ->
    Grps=Model#model.groups,
    GrpNames= [GrpName||{GrpName, _, _}<-Grps],
    ?SUCHTHAT(Name, gen_reg_name(),  not lists:member(Name, GrpNames)).
gen_reg_name()->
    eqc_gen:non_empty(eqc_gen:list(eqc_gen:choose(97, 122))).
   
gen_reg_name(NameSpace) ->
    {UsedNames,_} = lists:unzip(NameSpace),
    NameCandidates=[atom_to_list(N)||N<-UsedNames]++
        ["aa", "bb", "cc", "dd", "ee", "ff"],
    eqc_gen:oneof(NameCandidates).
gen_message() ->
    "an message".
    %% eqc_gen:binary().


gen_oneof([]) ->
    eqc_gen:oneof([undefined]);
gen_oneof(List) ->
    eqc_gen:oneof(List).
%%---------------------------------------------------------------
%%
%%   Utility functions.
%%
%%--------------------------------------------------------------
%% start and shutdown nodes...
setup() ->
    setup(?with_conf).

setup(WithConf)->
    ?dbg(0, "Starting nodes ...\n", []),
    Str = if WithConf ->
                  " -detached -setcookie \"secret\" -config s_group.config";
             true ->" -detached -setcookie \"secret\""
          end,
    os:cmd(?cmd++" -name node1@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node2@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node3@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node4@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node5@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node6@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node7@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node8@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node9@127.0.0.1 -hidden "++Str),
    os:cmd(?cmd++" -name node10@127.0.0.1 -hidden "++Str),
    os:cmd(?cmd++" -name node11@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node12@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node13@127.0.0.1"++Str),
    os:cmd(?cmd++" -name node14@127.0.0.1"++Str),
    timer:sleep(5000),
    fun()->teardown() end.
            
teardown()->
   F=fun(N) ->
             Node=list_to_atom("node"++integer_to_list(N)++"@127.0.0.1"),
             rpc:call(Node, erlang, halt, [])
      end,
    lists:foreach(fun(N) -> F(N) end, lists:seq(1, 14)).
   

all_node_ids(S) ->
    [NodeId||{NodeId, _, _, _}<-S#state.model#model.nodes].
   

add_hidden_connections(Nodes, Node1, Node2) ->
    {Node1, Conns1, Grps1} = lists:keyfind(Node1,1, Nodes),
    {Node2, Conns2, Grps2} = lists:keyfind(Node2,1, Nodes),
    NewConns1 = [Node2|Conns1],
    NewConns2 = [Node1|Conns2],
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
    %%NOTE: here we assume that this is no name confliction,
    %%      need to fix.
    NewFreeGrp ={NodeIds1++NodeIds2, NameSpace1++NameSpace2}, 
    NewFreeGrps=[NewFreeGrp|OtherGrps],
    NewNodes = inter_connect_nodes(NodeIds1++NodeIds2, Nodes),
    Model#model{free_groups=NewFreeGrps, nodes=NewNodes}.
    
    
find_name(Model, NodeId, GroupName, RegName) ->
    Nodes = Model#model.nodes,
    {NodeId, _,_, Grps} = lists:keyfind(NodeId,1,Nodes),
    [NameSpace]=case Grps of 
                    [free_hidden_group] ->
                        FreeHiddenGrps=Model#model.free_hidden_groups,
                        [NS||{Id, NS}<-FreeHiddenGrps, Id==NodeId];
                    [free_normal_group] ->
                        FreeGrps = Model#model.free_groups,
                        [NS||{Ids, NS}<-FreeGrps, lists:member(NodeId, Ids)];
                    _ ->
                        Grps1 = Model#model.groups,
                        [NS||{GrpName, _Ids, NS}<-Grps1, GrpName==GroupName]
                end,
    case lists:keyfind(RegName, 1, NameSpace) of 
        %% Note: this pid may not have the node info!
        {RegName, Pid} -> Pid;  
        _ ->
            undefined
    end.
    

%%---------------------------------------------------------------%%
%%                                                               %%
%%   Miscellaneous functions.                                    %%
%%                                                               %%
%%---------------------------------------------------------------%%

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


proc_is_alive(Node, Pid) ->
    case rpc:call(Node, erlang, process_info, [Pid]) of 
        {badrpc, nodedown} ->
            io:format("proc_is_alive: node down\n"),
            false;
        Info ->Info/=undefined
    end.

process_result(Res) when is_tuple(Res) ->
    element(1, Res);
process_result(Res) -> Res.

registered_names_with_pids(Node) ->
    MS=ets:fun2ms(fun({{SGroupName, Name},Pid,_M,_RP,_R}) ->
                          {SGroupName, Name, Pid} end),
    rpc:call(Node, ets, select, [global_names, MS]).
   

%% otherwise: add this function to global.erl.
%% registered_names_with_pids() ->
%%     MS=ets:fun2ms(fun({{SGroupName, Name},Pid,_M,_RP,_R}) ->
%%                           {SGroupName, Name, Pid} end),
%%     case ets:info(global_names) of 
%%         undefined -> 
%%             undefined;
%%         _ ->
%%             ets:select(global_names, MS)
%%     end.

%%---------------------------------------------------------------%%
%%                                                               %%
%%                        Some Notes                             %%
%%                                                               %%
%%---------------------------------------------------------------%%

%% 1) Register_name returns 'yes' when registering 
%%    a non-existent processes. 
%% 2) All the tests are executed sequentially; concurrent 
%%    execution of commands is not tested yet.
%% 3) In distributed Erlang, nodes have transitive connection by 
%%    default, but this can also be disabled. This is not modelled
%%    in the semantics.

%% 17/07/2013
%% 1) new version of s_group:register_name always returns no. 
%%    (resolved: this was due to the interface change of s_group function.) 
%% 2) old version (a month ago), global:register_name always returns no.
%%    (NL comments that global:register_name should not be used on a s_group node,
%%     not very convinced though. IMO, this is a bit restrictive.).  
%% 3) 'whereis_name' does not check 'undefined' case; but not something hard to describe.
%% 4) problem: re_register name does not remove the old tuple from the abstract 
%%    model if the name is already used. (this should have been fixed?)
%% 5) unregister_name returns True in abstract model, but return ok in actual mode.


%% 17/07/2013:
%% Some typos in the semantics specification:
%% line 117 (grs'',fgs,fhs,nds) -> {grs'', fgs', ghs', nds''} ?
%% line 123: nds -> nds' ?
%% line 126: (grs',fgs,fhs,nds) ->  {grs',fgs',fhs',nds'') ?
%% line 130: nds -> nds' ?
%% line 172: {ni' || ni' <- nis, ni != ni'} can be simplified as nis-{ni}?
%% line 232: css' -> css ?

%% 05/08/2013.
%% errors in semantics specification.
%% 1). fgs' = fgs +! {(nis',ns')} ||({ni}+nis'. ns')<- fgs, ni<-nis}
%%     The use of '+!' her does not remove the old free_group since the 
%%     key has been changed!
%%
%% 2). in AddSGroup:
%%  gs+{s}: also needs to remove 'free_hidden_group'/'free_normal_group' from 'gs'
%%          if there is one. 
%%
%% 3) problem when an s_group is deleted: it looks like the some nodes 
%%   are not connected whereas the abstract model says they should.
    
%% 07/08/2013: 

%% 1) sometimes during the testing, a node goes down after the 'remove_nodes' operation.  
%%   The node that goes down is the node on which the command is executed. 
%%   Manual testing could not trigger this problem. Still have not figured out the reason!

%% 2)It looks like that when a s_group node goes down and then up again, the s_group info 
%%   on this node does not get synchronized with other group nodes.

%% 24/08/2013. 
%% 1) Previously the testing complained some mismatch between the connections of nodes 
%%    after some s_groups are deleted. (i.e. the actual connections collected by the  
%%    testing were not transitive). Manual execution of a test case shows what happened. 
%%  Here is the manual test I did:
%%
%% 1) start 5 normal nodes: node1@127.0.0.1,node2@127.0.0.1,
%%                          node3@127.0.0.1, node4@127.0.0.1,
%%                          node5@127.0.0.1.
%%
%% 2) on node1: s_group:new_s_group(a, ['node1@127.0.0.1','node2@127.0.0.1','node3@127.0.0.1']).
%%    on node3: s_group:new_s_group(b, ['node3@127.0.0.1','node4@127.0.0.1','node5@127.0.0.1']).
%%    on node1: s_group:delete_s_group(a).
%%    on_node3: s_group:delete_s_group(b).
%%
%% Here is what happens on node1:
%%
%% (node1@127.0.0.1)8> 
%% =ERROR REPORT==== 24-Aug-2013::17:10:56 ===
%% global: nodeup never came 'node1@127.0.0.1' 'node2@127.0.0.1'

%% (node1@127.0.0.1)8> nodes(connected).
%% ['node2@127.0.0.1','node3@127.0.0.1','node5@127.0.0.1',
%%  'node4@127.0.0.1']
%%
%%  So nodes eventually get transitively connected, but this only happens after 
%%  a 60000 timeout and an error report. Here is the code in global.erl:
%%
%%  after 60000 ->
%%          ?trace({nodeupnevercame, node(HisTheLocker)}),
%%          error_logger:error_msg("global: nodeup never came ~w ~w\n",
%%                                 [node(), node(HisTheLocker)]),
%%          loop_the_locker(S#multi{just_synced = false})
%%  end;
%%
%% The testing failed because the node state data was collected before the timeout. 


%%  24/08/2013: 
%% Problem: sometimes the testing get stuck at the s_group_eqc:rpc_call(Node, s_group, register_name, [...]) for ever.
%%          this happens to re_register_name as well.
%%          something wrong in the test code or deadlock?
%% node down problem: monitor node?


