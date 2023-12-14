**RocketMq消费者（MQConsumerInner）的推、拉模式**
    消费者客户端有两种方式从消息中间件获取消息并消费。
    严格意义上来讲，RocketMQ并没有实现PUSH模式，而是对拉模式进行一层包装，名字虽然是 Push 开头，实际在实现时，使用 Pull 长轮询机制方式实现。
    通过 Pull 不断轮询 Broker 获取消息。当不存在新消息时，Broker 会挂起请求，直到有新消息产生，取消挂起，返回新消息。

PULL方式:
    由消费者客户端主动向消息中间件（MQ消息服务器代理）拉取消息；采用Pull方式，如何设置Pull消息的拉取频率需要重点去考虑。
    举个例子来说，可能1分钟内连续来了1000条消息，然后2小时内没有新消息产生（概括起来说就是“消息延迟与忙等待”）。
    如果每次Pull的时间间隔比较久，会增加消息的延迟，即消息到达消费者的时间加长，MQ中消息的堆积量变大；
    若每次Pull的时间间隔较短，但是在一段时间内MQ中并没有任何消息可以消费，那么会产生很多无效的Pull请求的RPC开销，影响MQ整体的网络性能；

PUSH方式
    由消息中间件（MQ消息服务器代理）主动地将消息推送给消费者,消费者将消息存在缓冲区。采用Push方式，可以尽可能实时地将消息发送给消费者进行消费。
    但是，在消费者的处理消息的能力较弱的时候(比如，消费者端的业务系统处理一条消息的流程比较复杂，其中的调用链路比较多导致消费时间比较久。
    概括起来地说就是“慢消费问题”)，而MQ不断地向消费者Push消息，消费者端的缓冲区可能会溢出，导致异常；
    
**RebalanceService触发Rebalance的条件：**
1、RebalanceService服务是一个线程任务，由MQClientInstance启动，其每隔20s自动进行一次自动负载均衡。
2、Broker触发的重平衡：
    -Broker收到心跳请求之后如果发现消息中有新的consumer连接或者consumer订阅了新的topic或者移除了topic的订阅， 
        则Broker发送Code为NOTIFY_CONSUMER_IDS_CHANGED的请求给该group下面的所有Consumer，要求进行一次负载均衡。
    -如果某个客户端连接出现连接异常事件EXCEPTION、连接断开事件CLOSE、或者连接闲置事件IDLE，则Broker同样会发送重平衡请求给消费者组下面的所有消费者。
3、新的Consumer服务启动的时候，主动调用rebalanceImmediately唤醒负载均衡服务rebalanceService，进行重平衡。



**MQ中6种AllocateMessageQueueStrategy负载均衡策略：**
1、AllocateMessageQueueAveragely：平均分配策略，
    这是默认策略。尽量将消息队列平均分配给所有消费者，多余的队列分配至排在前面的消费者。分配的时候，前一个消费者分配完了，才会给下一个消费者分配。
2、AllocateMessageQueueAveragelyByCircle：环形平均分配策略。
    尽量将消息队列平均分配给所有消费者，多余的队列分配至排在前面的消费者。与平均分配策略差不多，区别就是分配的时候，按照消费者的顺序进行一轮一轮的分配，直到分配完所有消息队列。
3、AllocateMessageQueueByConfig：根据用户配置的消息队列分配。将会直接返回用户配置的消息队列集合。
4、AllocateMessageQueueByMachineRoom：机房平均分配策略。
    消费者只消费绑定的机房中的broker，并对绑定机房中的MessageQueue进行负载均衡。
5、AllocateMachineRoomNearby：机房就近分配策略。消费者对绑定机房中的MessageQueue进行负载均衡。
    除此之外，对于某些拥有消息队列但却没有消费者的机房，其消息队列会被所有消费者分配，具体的分配策略是，另外传入的一个AllocateMessageQueueStrategy的实现。
6、AllocateMessageQueueConsistentHash：一致性哈希分配策略。基于一致性哈希算法分配。


**MQ中consumeMessageService的并发消费和顺序消费**
1、并发消费
并发消费是指多个消费者将并发消费消息，消费的时候可能是无序的

2、顺序消费
顺序消息是指对于一个指定的 Topic ，消息严格按照先进先出（FIFO）的原则进行消息发布和消费，即先发布的消息先消费，后发布的消息后消费

**RocketMQ消息过滤**
消息过滤包括基于《表达式过滤》与基于《类模式》两种过滤模式。其中表达式过滤又分为《TAG》和《SQL92模式》
地址：https://www.jianshu.com/p/00d010c8d1f5/

SQL过滤：在broker端进行，可以减少无用数据的网络传输但broker压力会大，性能低，支持使用SQL语句复杂的过滤逻辑。
TAG过滤：在broker与consumer端进行，增加无用数据的网络传输但broker压力小，性能高，只支持简单的过滤。

**notifyMessageArriving方法调用情况**
1、PullRequestHoldService线程定时调用：
    长轮询：最多挂起15s，每隔5s对所有PullRequest执行notifyMessageArriving方法。
    短轮询：最多挂起1s，每隔1s对所有PullRequest执行notifyMessageArriving方法。
2、ReputMessageService线程调用：
    当有新的消息到达时，在DefaultMessageStore#doReput方法对于新的消息执行重放的过程中，
    会对等待对应topic@queueId的所有PullRequest执行notifyMessageArriving方法。doReput方法每1ms执行一次。