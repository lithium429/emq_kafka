-module(emq_plugin_kafka).
 
-include_lib("emqttd/include/emqttd.hrl").
 
-define(APP, emq_plugin_kafka).
 
-export([load/1, unload/0]).
 
%% Hooks functions
 
-export([on_client_connected/3, on_client_disconnected/3]).
 
-export([on_client_subscribe/4, on_client_unsubscribe/4]).
 
-export([on_session_created/3, on_session_subscribed/4, on_session_unsubscribed/4, on_session_terminated/4]).
 
-export([on_message_publish/2, on_message_delivered/4, on_message_acked/4]).
 
%% Called when the plugin application start
load(Env) ->
    ekaf_init(Env),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqttd:hook('client.subscribe', fun ?MODULE:on_client_subscribe/4, [Env]),
    emqttd:hook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4, [Env]),
    emqttd:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqttd:hook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4, [Env]),
    emqttd:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    emqttd:hook('message.delivered', fun ?MODULE:on_message_delivered/4, [Env]),
    emqttd:hook('message.acked', fun ?MODULE:on_message_acked/4, [Env]).
 
on_client_connected(ConnAck, Client = #mqtt_client{client_id = ClientId}, _Env) ->
    io:format("client ~s connected, connack: ~w~n", [ClientId, ConnAck]),
    {ok, Client}.
 
on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId}, _Env) ->
    io:format("client ~s disconnected, reason: ~w~n", [ClientId, Reason]),
    ok.
 
on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("client(~s/~s) will subscribe: ~p~n", [Username, ClientId, TopicTable]),
    {ok, TopicTable}.
    
on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
    io:format("client(~s/~s) unsubscribe ~p~n", [ClientId, Username, TopicTable]),
    {ok, TopicTable}.
 
on_session_created(ClientId, Username, _Env) ->
    io:format("session(~s/~s) created.", [ClientId, Username]).
 
on_session_subscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    io:format("session(~s/~s) subscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
    {ok, {Topic, Opts}}.
 
on_session_unsubscribed(ClientId, Username, {Topic, Opts}, _Env) ->
    io:format("session(~s/~s) unsubscribed: ~p~n", [Username, ClientId, {Topic, Opts}]),
    ok.
 
on_session_terminated(ClientId, Username, Reason, _Env) ->
    io:format("session(~s/~s) terminated: ~p.", [ClientId, Username, Reason]).
 
%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};
 
on_message_publish(Message, _Env) ->
    io:format("publish ~s~n", [emqttd_message:format(Message)]),
    ekaf_send(Message, _Env),
    {ok, Message}.
 
on_message_delivered(ClientId, Username, Message, _Env) ->
    io:format("delivered to client(~s/~s): ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.
 
on_message_acked(ClientId, Username, Message, _Env) ->
    io:format("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
    {ok, Message}.
 
%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('client.subscribe', fun ?MODULE:on_client_subscribe/4),
    emqttd:unhook('client.unsubscribe', fun ?MODULE:on_client_unsubscribe/4),
    emqttd:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqttd:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqttd:unhook('session.unsubscribed', fun ?MODULE:on_session_unsubscribed/4),
    emqttd:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd:unhook('message.delivered', fun ?MODULE:on_message_delivered/4),
    emqttd:unhook('message.acked', fun ?MODULE:on_message_acked/4).
 
ekaf_init(_Env) ->
    {ok, Kafka_Env} = application:get_env(?APP, server),
    Host = proplists:get_value(host, Kafka_Env),
    Port = proplists:get_value(port, Kafka_Env),
    Broker = {Host, Port},
    %Broker = {"192.168.52.130", 9092},
    Topic = proplists:get_value(topic, Kafka_Env),
    %Topic = "test-topic",
    
    application:set_env(ekaf, ekaf_partition_strategy, strict_round_robin),
    application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
    application:set_env(ekaf, ekaf_bootstrap_topics, list_to_binary(Topic)),
    %%设置数据上报间隔，ekaf默认是数据达到1000条或者5秒，触发上报
    application:set_env(ekaf, ekaf_buffer_ttl, 100),
   
    {ok, _} = application:ensure_all_started(ekaf).
    %io:format("Init ekaf with ~p~n", [Broker]),
    %Json = mochijson2:encode([
    %    {type, <<"connected">>},
    %    {client_id, <<"test-client_id">>},
    %    {cluster_node, <<"node">>}
    %]),
    %io:format("send : ~w.~n",[ekaf:produce_async_batched(list_to_binary(Topic), list_to_binary(Json))]).
 
 
ekaf_send(Message, _Env) ->
    From = Message#mqtt_message.from, 
    Topic = Message#mqtt_message.topic,
    Payload = Message#mqtt_message.payload,
    Qos = Message#mqtt_message.qos,
    Dup = Message#mqtt_message.dup,
    Retain = Message#mqtt_message.retain,
    ClientId = get_form_clientid(From),
    Username = get_form_username(From),
    io:format("message receive : ~n",[]),
    io:format("From : ~w~n",[From]),
    io:format("Topic : ~w~n",[Topic]),
    io:format("Payload : ~w~n",[Payload]),
    io:format("Qos : ~w~n",[Qos]),    
    io:format("Dup : ~w~n",[Dup]),
    io:format("Retain : ~w~n",[Retain]),
    io:format("ClientId : ~w~n",[ClientId]),
    io:format("Username : ~w~n",[Username]),
    Str = [
              {client_id, ClientId},
              {message, [
                            {username, Username},
                            {topic, Topic},
                            {payload, binary_to_list(<<Payload/binary>>)},
                            {qos, Qos},
                            {dup, Dup},
                            {retain, Retain}
                        ]},
               {cluster_node, node()},
               {ts, emqttd_time:now_ms()}
           ],
    io:format("Str : ~w.~n", [Str]),
    Json = mochijson2:encode(Str),
    KafkaTopic = get_topic(),
    ekaf:produce_sync_batched(KafkaTopic, list_to_binary(Json)).
 
get_form_clientid({ClientId, Username}) -> ClientId;
get_form_clientid(From) -> From.
get_form_username({ClientId, Username}) -> Username;
get_form_username(From) -> From.
 
get_topic() -> 
    {ok, Topic} = application:get_env(ekaf, ekaf_bootstrap_topics),
    Topic.
