/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 3000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    private final long consumerStartTimestamp = System.currentTimeMillis();
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private final RPCHook rpcHook;
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;
    private volatile boolean pause = false;
    private boolean consumeOrderly = false;
    private MessageListener messageListenerInner;
    private OffsetStore offsetStore;
    private ConsumeMessageService consumeMessageService;
    private long queueFlowControlTimes = 0;
    private long queueMaxSpanFlowControlTimes = 0;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        //推送失败后，延迟多少ms再次推送。默认1000ms
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return parseSubscribeMessageQueues(result);
    }

    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public void pullMessage(final PullRequest pullRequest) {
        //获取对应的处理队列
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        //如果该处理队列已被丢去，那么直接返回
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        //设置最后的拉取时间戳
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        try {
            //确定此consumer的服务状态正常，如果服务状态不是RUNNING，那么抛出异常
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }

        //如果消费者暂停了
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            //那么延迟1s发送拉取消息请求
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }
        //流控处理

        //获取processQueue中已缓存的消息总数量
        long cachedMessageCount = processQueue.getMsgCount().get();
        //获取processQueue中已缓存的消息总大小MB
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

        //如果processQueue中已缓存的消息总数量大于设定的阈值
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            //延迟50ms发送拉取消息请求
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        //如果processQueue中已缓存的消息总大小大于设定的阈值
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            //延迟50ms发送拉取消息请求
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                        "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                        this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        //不是有序消费。即：并发消费
        if (!this.consumeOrderly) {
            //如果内存中消息的offset的最大跨度大于设定的阈值，默认2000
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                //延迟50ms发送拉取消息请求
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                            "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                            processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                            pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        } else {
            //顺序消费
            //如果processQueue已经锁定
            if (processQueue.isLocked()) {
                //如果此前没有锁定过，表示之前的pullRequest请求不是顺序消费请求，或者请求无效。
                //则需要设置消费offset偏移量
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        //获取该MessageQueue的下一个消息的消费偏移量offset
                        //pull模式返回0，push模式则根据ConsumeFromWhere枚举计算得到
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                    } catch (Exception e) {
                        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }
                    //消费点位超前，那么重设消费点位
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                            pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                                pullRequest, offset);
                    }

                    //设置previouslyLocked为true
                    pullRequest.setPreviouslyLocked(true);
                    //重新设置消费点位
                    pullRequest.setNextOffset(offset);
                }
            } else {
                //如果没有被锁住，那么延迟3s发送拉取消息请求
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }

        //获取topic对应的SubscriptionData订阅关系
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        //如果没有订阅信息
        if (null == subscriptionData) {
            //延迟3s发送拉取消息请求
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        // 创建拉取消息的回调函数对象，当拉取消息的请求返回之后，将会指定回调函数
        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                            subscriptionData);

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            long prevRequestOffset = pullRequest.getNextOffset();
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullRT);

                            long firstMsgOffset = Long.MAX_VALUE;
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            } else {
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                        pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                        pullResult.getMsgFoundList(),
                                        processQueue,
                                        pullRequest.getMessageQueue(),
                                        dispatchToConsume);

                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                            DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                                } else {
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                    || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                        "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                        pullResult.getNextBeginOffset(),
                                        firstMsgOffset,
                                        prevRequestOffset);
                            }

                            break;
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal, {} {}",
                                    pullRequest.toString(), pullResult.toString());
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            pullRequest.getProcessQueue().setDropped(true);
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                                pullRequest.getNextOffset(), false);

                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());

                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }

                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };

        //是否允许提交偏移量到Broker
        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        //集群模式下
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            //从本地内存offsetTable读取commitOffsetValue
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                //如果本地内存有关于此mq的offset，那么设置为true，表示可以上报消费位点给Broker
                commitOffsetEnable = true;
            }
        }

        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            classFilter = sd.isClassFilterMode();
        }

        //二进制或操作
        int sysFlag = PullSysFlag.buildSysFlag(
                commitOffsetEnable, // commitOffset
                true, // suspend
                subExpression != null, // subscription
                classFilter // class filter
        );
        try {
            //真正的开始拉取消息
            this.pullAPIWrapper.pullKernelImpl(
                    pullRequest.getMessageQueue(),
                    subExpression,
                    subscriptionData.getExpressionType(),
                    subscriptionData.getSubVersion(),
                    pullRequest.getNextOffset(),
                    this.defaultMQPushConsumer.getPullBatchSize(),
                    sysFlag,
                    commitOffsetValue,
                    BROKER_SUSPEND_MAX_TIME_MILLIS,
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                    CommunicationMode.ASYNC,
                    pullCallback
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            //拉取异常，延迟3s发送拉取消息请求
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
        }
    }

    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
            InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                    : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                    this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());
        } catch (Exception e) {
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    private int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public void shutdown() {
        shutdown(0);
    }

    public synchronized void shutdown(long awaitTerminateMillis) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown(awaitTerminateMillis);
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    //synchronized只能一个线程启动
    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                        this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                this.serviceState = ServiceState.START_FAILED;

                //校验consumerGroup是否符合要求
                //如果consumerGroup为空，或者长度大于255个字符，或者包含非法字符（正常的匹配模式为 ^[%|a-zA-Z0-9_-]+$），或者消费者组名为默认组名DEFAULT_CONSUMER
                //或者messageModel为空，或者consumeFromWhere为空，或者consumeTimestamp为空，或者allocateMessageQueueStrategy为空等等属性的空校验
                //满足以上任意条件都校验不通过抛出异常
                this.checkConfig();

                //复制订阅关系到rebalanceImpl + 根据消费者消费消息的模式，做出不同的消费失败策略
                this.copySubscription();

                //如果是集群消费模式，如果instanceName为默认值 "DEFAULT"
                // 那么InstanceName改成 UtilAll.getPid() + "#" + System.nanoTime()
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    /**
                     * RocketMQ用一个叫ClientID的概念，来唯一标记一个客户端实例，一个客户端实例对于Broker而言会开辟一个Netty的客户端实例。
                     * 而ClientID是由ClientIP+InstanceName构成，故如果一个进程中多个实例（无论Producer还是Consumer）ClientIP和InstanceName都一样,
                     * 他们将公用一个内部实例（同一套网络连接，线程资源等）
                     *
                     * 此ClientID在对于Consumer负载均衡的时候起到唯一标识的作用，
                     * 一旦多个实例（无论不同进程、不通机器、还是同一进程）的多个Consumer实例有一样的ClientID，
                     * 负载均衡的时候必然RocketMQ任然会把两个实例当作一个client（因为同样一个clientID）。有可能导致两个实例pull到相同的消息了
                     * 故为了避免不必要的问题，ClientIP+instance Name的组合建议唯一，除非有意需要共用连接、资源。
                     */
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }

                /*
                 * 获取MQClientManager实例，然后根据clientId获取或者创建CreateMQClientInstance实例，并赋给mQClientFactory变量
                 *
                 * MQClientInstance封装了RocketMQ底层网络处理API，Producer、Consumer都会使用到这个类，是Producer、Consumer与NameServer、Broker 打交道的网络通道。
                 * 因此，同一个clientId对应同一个MQClientInstance实例就可以了，即同一个应用中的多个producer和consumer使用同一个MQClientInstance实例即可。
                 */
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                /*
                 * 设置负载均衡服务的相关属性
                 */
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);

                //创建消息拉取核心对象PullAPIWrapper，封装了消息拉取及结果解析逻辑的API
                this.pullAPIWrapper = new PullAPIWrapper(
                        mQClientFactory,
                        this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                //PullAPIWrapper注册过滤消息的钩子函数
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                //根据消息模式设置不同的OffsetStore，用于实现消费者的消息消费偏移量offset的管理
                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    //当前defaultMQPushConsumer中存了偏移量，则直接存本地
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            //广播模式，直接存本地磁盘中
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            //集群模式，则是RemoteBrokerOffsetStore，消息消费进度即offset存储在远程broker中
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }
                //加载消费偏移量，LocalFileOffsetStore会加载本地磁盘中的数据，RemoteBrokerOffsetStore则是一个空实现
                this.offsetStore.load();

                //根据消息监听器的类型创建不同的消息消费服务
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    //有序消费消息，则创建ConsumeMessageOrderlyService
                    this.consumeOrderly = true;
                    this.consumeMessageService =
                            new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    //如果是MessageListenerConcurrently类型，则表示并发消费，创建ConsumeMessageOrderlyService
                    this.consumeOrderly = false;
                    this.consumeMessageService =
                            new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }

                //启动消息消费服务consumeMessageService
                this.consumeMessageService.start();

                //注册消费者组和消费者到MQClientInstance中的consumerTable中
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    //如果没注册成功，那么可能是因为同一个程序中存在同名消费者组consumerGroup的不同消费者
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                            + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                            null);
                }

                //启动CreateMQClientInstance客户端通信实例
                //netty服务、各种定时任务、拉取消息服务、rebalanceService服务
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }

        //向NameServer拉取并更新当前消费者订阅的topic路由信息
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        //随机选择一个Broker，发送检查客户端tag配置的请求，主要是检测Broker是否支持SQL92类型的tag过滤以及SQL92的tag语法是否正确
        this.mQClientFactory.checkClientInBroker();

        //发送心跳信息给所有broker
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        //唤醒负载均衡服务rebalanceService，进行重平衡
        this.mQClientFactory.rebalanceImmediately();
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());

        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                    "consumerGroup is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                    "consumerGroup can not equal "
                            + MixAll.DEFAULT_CONSUMER_GROUP
                            + ", please specify another one."
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                    "messageModel is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                    "consumeFromWhere is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                    "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                            + this.defaultMQPushConsumer.getConsumeTimestamp()
                            + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // subscription
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                    "subscription is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                    "messageListener is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                    "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
                || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException(
                    "consumeThreadMin Out of range [1, 1000]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                    "consumeThreadMax Out of range [1, 1000]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeThreadMin can't be larger than consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                    "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                            + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                    null);
        }

        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
                || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                    "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                    "pullThresholdForQueue Out of range [1, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullThresholdForTopic
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                        "pullThresholdForTopic Out of range [1, 6553500]"
                                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullThresholdSizeForQueue
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                    "pullThresholdSizeForQueue Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                        "pullThresholdSizeForTopic Out of range [1, 102400]"
                                + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                        null);
            }
        }

        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                    "pullInterval Out of range [0, 65535]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
                || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                    "consumeMessageBatchMaxSize Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                    "pullBatchSize Out of range [1, 1024]"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
    }

    private void copySubscription() throws MQClientException {
        try {
            ////将订阅关系拷贝到RebalanceImpl的subscriptionInner中
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            // 如果messageListenerInner为null，那么将defaultMQPushConsumer的messageListener
            // 赋给DefaultMQPushConsumerImpl的messageListenerInner
            //之前在defaultMQPushConsumer的registerMessageListener方法中就已经赋值了
            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            //重要！！！
            //根据消费者消费消息的模式，做出不同的消费失败策略
            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    //广播，消费失败直接抛弃消息
                    break;
                case CLUSTERING:
                    //集群消费，支持消费失败重试
                    //创建消费失败的重试retryTopic = %RETRY% + consumerGroup
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    //当前消费者自动订阅该消费者组对应的重试topic，用于实现消费重试
                    //消息消费失败或超时,会被投递到 RETRY topic，并不会重新投递到当前topic！！！
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    //复制到rebalanceImpl中
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            //解析订阅表达式，构建SubscriptionData
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);
            //将topic与SubscriptionData的关系维护到RebalanceImpl内部的subscriptionInner这个map集合中
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                //如果mQClientFactory不为null，则发送心跳信息给所有broker。
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }

            SubscriptionData subscriptionData = FilterAPI.build(topic,
                    messageSelector.getExpression(), messageSelector.getExpressionType());

            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    @Override
    public void doRebalance() {
        if (!this.pause) {
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
            throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }

            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }
}
