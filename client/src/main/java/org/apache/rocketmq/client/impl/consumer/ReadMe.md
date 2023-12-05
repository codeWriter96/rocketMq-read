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