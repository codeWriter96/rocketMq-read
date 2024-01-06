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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

//延迟消息
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //启动时，首次延迟时间
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;
    private static final long WAIT_FOR_SHUTDOWN = 5000L;
    private static final long DELAY_FOR_A_SLEEP = 10L;

    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    //delayOffset，延迟队列消费偏移量缓存 -> 延迟消息文件${user.home}/store/config/delayOffset.json
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);
    private final DefaultMessageStore defaultMessageStore;
    private final AtomicBoolean started = new AtomicBoolean(false);
    //延迟消息投递线程池，核心线程数就是最大的延迟等级，默认18
    private ScheduledExecutorService deliverExecutorService;
    private MessageStore writeMessageStore;
    //最大延迟等级
    private int maxDelayLevel;
    //是否异步投递消息，默认false
    private boolean enableAsyncDeliver = false;
    private ScheduledExecutorService handleExecutorService;
    private final Map<Integer /* level */, LinkedBlockingQueue<PutResultProcess>> deliverPendingTable =
        new ConcurrentHashMap<>(32);

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
        if (defaultMessageStore != null) {
            this.enableAsyncDeliver = defaultMessageStore.getMessageStoreConfig().isEnableScheduleAsyncDeliver();
        }
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    //启动延迟队列
    public void start() {
        //compareAndSet将未启动改为启动，服务只能启动一次
        if (started.compareAndSet(false, true)) {
            //调用父类的load方法，将延迟消息文件${user.home}/store/config/delayOffset.json加载到内存的offsetTable集合中
            super.load();
            //初始化延迟消息投递线程池，核心线程数就是最大的延迟等级，默认18
            this.deliverExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageTimerThread_"));
            //异步投递，默认不支持
            if (this.enableAsyncDeliver) {
                this.handleExecutorService = new ScheduledThreadPoolExecutor(this.maxDelayLevel, new ThreadFactoryImpl("ScheduleMessageExecutorHandleThread_"));
            }
            //对所有的延迟等级构建一个对应的DeliverDelayedMessageTimerTask调度任务，默认延迟1000ms后执行
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                //延迟等级
                Integer level = entry.getKey();
                //延迟时间
                Long timeDelay = entry.getValue();
                //消费偏移量。根据延迟等级获取对应的延迟队列的消费偏移量，如果没有则设置为0
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                //延迟时间不为null，那么为该等级的延迟队列构建一个DeliverDelayedMessageTimerTask调度任务，默认延迟1000ms后执行
                if (timeDelay != null) {
                    if (this.enableAsyncDeliver) {
                        this.handleExecutorService.schedule(new HandlePutResultTask(level), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                    }
                    //构建一个DeliverDelayedMessageTimerTask调度任务，默认延迟1000ms后执行
                    this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME, TimeUnit.MILLISECONDS);
                }
            }

            /*
             * 构建一个延迟队列消费偏移量持久化的定时调度任务，首次延迟1000ms之后执行，后续每次执行间隔flushDelayOffsetInterval时间，默认10s
             */
            this.deliverExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (started.get()) {
                            ScheduleMessageService.this.persist();
                        }
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval(), TimeUnit.MILLISECONDS);
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false) && null != this.deliverExecutorService) {
            this.deliverExecutorService.shutdown();
            try {
                this.deliverExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.error("deliverExecutorService awaitTermination error", e);
            }

            if (this.handleExecutorService != null) {
                this.handleExecutorService.shutdown();
                try {
                    this.handleExecutorService.awaitTermination(WAIT_FOR_SHUTDOWN, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    log.error("handleExecutorService awaitTermination error", e);
                }
            }

            if (this.deliverPendingTable != null) {
                for (int i = 1; i <= this.deliverPendingTable.size(); i++) {
                    log.warn("deliverPendingTable level: {}, size: {}", i, this.deliverPendingTable.get(i).size());
                }
            }

            this.persist();
        }
    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    @Override
    public String encode() {
        return this.encode(false);
    }

    //加载延迟队列到缓存
    @Override
    public boolean load() {
        //调用父类ConfigManager#load方法，将延迟消息文件${user.home}/store/config/delayOffset.json加载到内存的offsetTable集合中
        //delayOffset.json中保存着延迟topic每个队列的消费进度（消费偏移量）
        boolean result = super.load();
        //解析延迟级别到delayLevelTable集合中
        result = result && this.parseDelayLevel();
        //矫正每个延迟队列的偏移量
        result = result && this.correctDelayOffset();
        return result;
    }

    //矫正offsetTable的消费偏移量。以ConsumeQueue中偏移量为准
    public boolean correctDelayOffset() {
        try {
            //遍历延迟等级缓存
            for (int delayLevel : delayLevelTable.keySet()) {
                //获取延迟等级对应的消费队列，topic为 SCHEDULE_TOPIC_XXXX ，queueId为 delayLevel - 1
                ConsumeQueue cq =
                    ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                        delayLevel2QueueId(delayLevel));
                //缓存获取消息偏移量
                Long currentDelayOffset = offsetTable.get(delayLevel);
                if (currentDelayOffset == null || cq == null) {
                    continue;
                }
                long correctDelayOffset = currentDelayOffset;
                //延迟队列中获取最小偏移量
                long cqMinOffset = cq.getMinOffsetInQueue();
                //最大偏移量
                long cqMaxOffset = cq.getMaxOffsetInQueue();
                //当前偏移量 < 最小偏移量，correctDelayOffset置为最小偏移量
                if (currentDelayOffset < cqMinOffset) {
                    correctDelayOffset = cqMinOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }

                //当前偏移量 > 最大偏移量，correctDelayOffset置为最大偏移量
                if (currentDelayOffset > cqMaxOffset) {
                    correctDelayOffset = cqMaxOffset;
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, cqMaxOffset={}, queueId={}",
                        currentDelayOffset, cqMinOffset, cqMaxOffset, cq.getQueueId());
                }
                //修改offsetTable的消费偏移量为correctDelayOffset
                if (correctDelayOffset != currentDelayOffset) {
                    log.error("correct delay offset [ delayLevel {} ] from {} to {}", delayLevel, currentDelayOffset, correctDelayOffset);
                    offsetTable.put(delayLevel, correctDelayOffset);
                }
            }
        } catch (Exception e) {
            log.error("correctDelayOffset exception", e);
            return false;
        }
        return true;
    }

    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    @Override
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    //解析延迟等级到delayLevelTable中
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        //从MessageStoreConfig中获取延迟等级字符串messageDelayLevel
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            //空格拆分
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                //获取延迟的值
                String value = levelArray[i];
                //获取延迟单位
                String ch = value.substring(value.length() - 1);
                //从table中获取对应的延迟单位的时间毫秒
                Long tu = timeUnitTable.get(ch);

                //延迟等级 1开始
                int level = i + 1;
                //最大延迟等级修正
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                //延迟时间
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                //计算后的延迟时间 ms
                long delayTimeMillis = tu * num;
                //存入delayLevelTable中
                this.delayLevelTable.put(level, delayTimeMillis);
                if (this.enableAsyncDeliver) {
                    this.deliverPendingTable.put(level, new LinkedBlockingQueue<>());
                }
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    //构建内部消息对象，从延迟消息恢复成原始队列消息
    // 设置topic为REAL_TOPIC属性值，即原始topic，
    // 设置queueId为REAL_QID属性值，即原始queueId。即恢复为正常消息
    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        //构建msgInner
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        //还原真正的tagCode值
        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
            MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);

        return msgInner;
    }

    //投递延迟消息任务
    class DeliverDelayedMessageTimerTask implements Runnable {
        //消息延迟等级
        private final int delayLevel;
        //当前已消费偏移量
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        @Override
        //运行 延迟消息投递任务
        public void run() {
            try {
                //如果服务已启动，那么继续执行
                if (isStarted()) {
                    //执行延迟消息投递
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                //抛出异常，新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，10000ms后执行，本次任务结束
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * 纠正投递时间
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            //投递时间
            long result = deliverTimestamp;

            //最大时间戳 = 当前时间 + 延迟时间
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            //如果投递时间 > 最大时间戳
            if (deliverTimestamp > maxTimestamp) {
                //投递时间改为当前时间
                result = now;
            }

            return result;
        }

        //执行延迟消息投递
        public void executeOnTimeup() {
            //根据topic和延迟队列id从consumeQueueTable查找需要写入的ConsumeQueue，如果没找到就新建，即ConsumeQueue文件是延迟创建的
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            /*
             * 如果没找到对应的消息队列，新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，100ms后执行，本次任务结束
             */
            if (cq == null) {
                this.scheduleNextTimerTask(this.offset, DELAY_FOR_A_WHILE);
                return;
            }

            /*
             * 根据逻辑offset定位到物理偏移量，然后截取该偏移量之后的一段Buffer，其包含要拉取的消息的索引数据及对应consumeQueue文件之后的全部索引数据。
             * 这里截取的Buffer可能包含多条索引数据，因为需要批量拉取多条消息，以及进行消息过滤。
             */
            SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
            //没获取到缓存buffer
            if (bufferCQ == null) {
                long resetOffset;
                //如果当前消息队列的最小偏移量 大于 当前偏移量，那么当前偏移量无效，设置新的offset为最小偏移量
                if ((resetOffset = cq.getMinOffsetInQueue()) > this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMinOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                }
                //如果当前消息队列的最大偏移量 小于 当前偏移量，那么当前偏移量无效，设置新的offset为最大偏移量
                else if ((resetOffset = cq.getMaxOffsetInQueue()) < this.offset) {
                    log.error("schedule CQ offset invalid. offset={}, cqMaxOffset={}, queueId={}",
                        this.offset, resetOffset, cq.getQueueId());
                } else {
                    resetOffset = this.offset;
                }

                //新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，指定新的消费偏移量，100ms后执行，本次任务结束
                this.scheduleNextTimerTask(resetOffset, DELAY_FOR_A_WHILE);
                return;
            }

            //获取到缓存buffer
            /*
             * 遍历缓存buffer中的消息，根据tagsCode投递时间判断消息是否到期，
             * 如果到期则回复真实消息并且投递到真实topic以及对应的queueId中
             */
            long nextOffset = this.offset;
            try {
                //i表示总consumeQueue消息索引大小 = 消息数量 * CQ_STORE_UNIT_SIZE 字节
                int i = 0;
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                //遍历截取的Buffer中的consumeQueue消息索引，固定长度20b
                for (; i < bufferCQ.getSize() && isStarted(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    //8字节消息在commitlog文件中的物理偏移量
                    long offsetPy = bufferCQ.getByteBuffer().getLong();
                    //4字节消息大小
                    int sizePy = bufferCQ.getByteBuffer().getInt();
                    //8字节消息的tagsCode
                    //对于延迟消息，tagsCode被替换为延迟消息的未来发送时间（CommitLog#checkMessageAndReturnSize方法中）
                    long tagsCode = bufferCQ.getByteBuffer().getLong();

                    //如果tagsCode是扩展文件地址
                    if (cq.isExtAddr(tagsCode)) {
                        if (cq.getExt(tagsCode, cqExtUnit)) {
                            tagsCode = cqExtUnit.getTagsCode();
                        } else {
                            //can't find ext content.So re compute tags code.
                            log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                tagsCode, offsetPy, sizePy);
                            long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                            tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                        }
                    }

                    //当前时间
                    long now = System.currentTimeMillis();
                    //校验投递时间，必须 <= 当前时间 + 延迟时间
                    long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);
                    nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                    //如果投递时间大于当前时间，那么新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，指定新的消费偏移量，100ms后执行，本次任务结束
                    //消息消费
                    long countdown = deliverTimestamp - now;
                    if (countdown > 0) {
                        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                        return;
                    }

                    //根据消息物理偏移量从commitLog中找到该条消息。
                    MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(offsetPy, sizePy);
                    if (msgExt == null) {
                        continue;
                    }

                    //构建内部消息对象，
                    // 设置topic为REAL_TOPIC属性值，即原始topic，
                    // 设置queueId为REAL_QID属性值，即原始queueId。即恢复为正常消息
                    MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                    if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                        log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                            msgInner.getTopic(), msgInner);
                        continue;
                    }

                    //消息投递。bug-> offset应该改为nextOffset
                    boolean deliverSuc;
                    if (ScheduleMessageService.this.enableAsyncDeliver) {
                        //异步投递。默认不支持
                        deliverSuc = this.asyncDeliver(msgInner, msgExt.getMsgId(), offset, offsetPy, sizePy);
                    } else {
                        //默认同步投递。
                        deliverSuc = this.syncDeliver(msgInner, msgExt.getMsgId(), offset, offsetPy, sizePy);
                    }

                    //如果投递失败，那么新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，
                    // 指定新的消费偏移量，100ms后执行，本次任务结束
                    if (!deliverSuc) {
                        this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
                        return;
                    }
                }

                //遍历结束，更新下一个offset
                nextOffset = this.offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            } catch (Exception e) {
                log.error("ScheduleMessageService, messageTimeup execute error, offset = {}", nextOffset, e);
            } finally {
                bufferCQ.release();
            }

            /*
             * 新建一个DeliverDelayedMessageTimerTask任务存入deliverExecutorService，指定新的消费偏移量，100ms后执行，本次任务结束
             *
             * 保证线程任务的活性
             */
            this.scheduleNextTimerTask(nextOffset, DELAY_FOR_A_WHILE);
        }

        public void scheduleNextTimerTask(long offset, long delay) {
            ScheduleMessageService.this.deliverExecutorService.schedule(new DeliverDelayedMessageTimerTask(
                this.delayLevel, offset), delay, TimeUnit.MILLISECONDS);
        }

        //同步投递消息
        private boolean syncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            //投递消息，内部调用asyncPutMessage方法
            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, false);
            //投递结果
            PutMessageResult result = resultProcess.get();
            boolean sendStatus = result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK;
            if (sendStatus) {
                //如果发送成功，那么更新offsetTable中的消费偏移量
                ScheduleMessageService.this.updateOffset(this.delayLevel, resultProcess.getNextOffset());
            }
            return sendStatus;
        }

        private boolean asyncDeliver(MessageExtBrokerInner msgInner, String msgId, long offset, long offsetPy,
            int sizePy) {
            Queue<PutResultProcess> processesQueue = ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            //Flow Control
            int currentPendingNum = processesQueue.size();
            int maxPendingLimit = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxPendingLimit();
            if (currentPendingNum > maxPendingLimit) {
                log.warn("Asynchronous deliver triggers flow control, " +
                    "currentPendingNum={}, maxPendingLimit={}", currentPendingNum, maxPendingLimit);
                return false;
            }

            //Blocked
            PutResultProcess firstProcess = processesQueue.peek();
            if (firstProcess != null && firstProcess.need2Blocked()) {
                log.warn("Asynchronous deliver block. info={}", firstProcess.toString());
                return false;
            }

            PutResultProcess resultProcess = deliverMessage(msgInner, msgId, offset, offsetPy, sizePy, true);
            processesQueue.add(resultProcess);
            return true;
        }

        private PutResultProcess deliverMessage(MessageExtBrokerInner msgInner, String msgId, long offset,
            long offsetPy, int sizePy, boolean autoResend) {
            CompletableFuture<PutMessageResult> future =
                ScheduleMessageService.this.writeMessageStore.asyncPutMessage(msgInner);
            return new PutResultProcess()
                .setTopic(msgInner.getTopic())
                .setDelayLevel(this.delayLevel)
                .setOffset(offset)
                .setPhysicOffset(offsetPy)
                .setPhysicSize(sizePy)
                .setMsgId(msgId)
                .setAutoResend(autoResend)
                .setFuture(future)
                .thenProcess();
        }
    }

    public class HandlePutResultTask implements Runnable {
        private final int delayLevel;

        public HandlePutResultTask(int delayLevel) {
            this.delayLevel = delayLevel;
        }

        @Override
        public void run() {
            LinkedBlockingQueue<PutResultProcess> pendingQueue =
                ScheduleMessageService.this.deliverPendingTable.get(this.delayLevel);

            PutResultProcess putResultProcess;
            while ((putResultProcess = pendingQueue.peek()) != null) {
                try {
                    switch (putResultProcess.getStatus()) {
                        case SUCCESS:
                            ScheduleMessageService.this.updateOffset(this.delayLevel, putResultProcess.getNextOffset());
                            pendingQueue.remove();
                            break;
                        case RUNNING:
                            break;
                        case EXCEPTION:
                            if (!isStarted()) {
                                log.warn("HandlePutResultTask shutdown, info={}", putResultProcess.toString());
                                return;
                            }
                            log.warn("putResultProcess error, info={}", putResultProcess.toString());
                            putResultProcess.onException();
                            break;
                        case SKIP:
                            log.warn("putResultProcess skip, info={}", putResultProcess.toString());
                            pendingQueue.remove();
                            break;
                    }
                } catch (Exception e) {
                    log.error("HandlePutResultTask exception. info={}", putResultProcess.toString(), e);
                    putResultProcess.onException();
                }
            }

            if (isStarted()) {
                ScheduleMessageService.this.handleExecutorService
                    .schedule(new HandlePutResultTask(this.delayLevel), DELAY_FOR_A_SLEEP, TimeUnit.MILLISECONDS);
            }
        }
    }

    public class PutResultProcess {
        private String topic;
        private long offset;
        private long physicOffset;
        private int physicSize;
        private int delayLevel;
        private String msgId;
        private boolean autoResend = false;
        private CompletableFuture<PutMessageResult> future;

        private volatile int resendCount = 0;
        private volatile ProcessStatus status = ProcessStatus.RUNNING;

        public PutResultProcess setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PutResultProcess setOffset(long offset) {
            this.offset = offset;
            return this;
        }

        public PutResultProcess setPhysicOffset(long physicOffset) {
            this.physicOffset = physicOffset;
            return this;
        }

        public PutResultProcess setPhysicSize(int physicSize) {
            this.physicSize = physicSize;
            return this;
        }

        public PutResultProcess setDelayLevel(int delayLevel) {
            this.delayLevel = delayLevel;
            return this;
        }

        public PutResultProcess setMsgId(String msgId) {
            this.msgId = msgId;
            return this;
        }

        public PutResultProcess setAutoResend(boolean autoResend) {
            this.autoResend = autoResend;
            return this;
        }

        public PutResultProcess setFuture(CompletableFuture<PutMessageResult> future) {
            this.future = future;
            return this;
        }

        public String getTopic() {
            return topic;
        }

        public long getOffset() {
            return offset;
        }

        public long getNextOffset() {
            return offset + 1;
        }

        public long getPhysicOffset() {
            return physicOffset;
        }

        public int getPhysicSize() {
            return physicSize;
        }

        public Integer getDelayLevel() {
            return delayLevel;
        }

        public String getMsgId() {
            return msgId;
        }

        public boolean isAutoResend() {
            return autoResend;
        }

        public CompletableFuture<PutMessageResult> getFuture() {
            return future;
        }

        public int getResendCount() {
            return resendCount;
        }

        public PutResultProcess thenProcess() {
            this.future.thenAccept(result -> {
                this.handleResult(result);
            });

            this.future.exceptionally(e -> {
                log.error("ScheduleMessageService put message exceptionally, info: {}",
                    PutResultProcess.this.toString(), e);

                onException();
                return null;
            });
            return this;
        }

        private void handleResult(PutMessageResult result) {
            if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                onSuccess(result);
            } else {
                log.warn("ScheduleMessageService put message failed. info: {}.", result);
                onException();
            }
        }

        public void onSuccess(PutMessageResult result) {
            this.status = ProcessStatus.SUCCESS;
            if (ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleMessageStats()) {
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incQueueGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, delayLevel - 1, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetNums(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getMsgNum());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incGroupGetSize(MixAll.SCHEDULE_CONSUMER_GROUP, TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutNums(this.topic, result.getAppendMessageResult().getMsgNum(), 1);
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutSize(this.topic, result.getAppendMessageResult().getWroteBytes());
                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incBrokerPutNums(result.getAppendMessageResult().getMsgNum());
            }
        }

        public void onException() {
            log.warn("ScheduleMessageService onException, info: {}", this.toString());
            if (this.autoResend) {
                this.resend();
            } else {
                this.status = ProcessStatus.SKIP;
            }
        }

        public ProcessStatus getStatus() {
            return this.status;
        }

        public PutMessageResult get() {
            try {
                return this.future.get();
            } catch (InterruptedException | ExecutionException e) {
                return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
            }
        }

        private void resend() {
            log.info("Resend message, info: {}", this.toString());

            // Gradually increase the resend interval.
            try {
                Thread.sleep(Math.min(this.resendCount++ * 100, 60 * 1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            try {
                MessageExt msgExt = ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(this.physicOffset, this.physicSize);
                if (msgExt == null) {
                    log.warn("ScheduleMessageService resend not found message. info: {}", this.toString());
                    this.status = need2Skip() ? ProcessStatus.SKIP : ProcessStatus.EXCEPTION;
                    return;
                }

                MessageExtBrokerInner msgInner = ScheduleMessageService.this.messageTimeup(msgExt);
                PutMessageResult result = ScheduleMessageService.this.writeMessageStore.putMessage(msgInner);
                this.handleResult(result);
                if (result != null && result.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    log.info("Resend message success, info: {}", this.toString());
                }
            } catch (Exception e) {
                this.status = ProcessStatus.EXCEPTION;
                log.error("Resend message error, info: {}", this.toString(), e);
            }
        }

        public boolean need2Blocked() {
            int maxResendNum2Blocked = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked;
        }

        public boolean need2Skip() {
            int maxResendNum2Blocked = ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig()
                .getScheduleAsyncDeliverMaxResendNum2Blocked();
            return this.resendCount > maxResendNum2Blocked * 2;
        }

        @Override
        public String toString() {
            return "PutResultProcess{" +
                "topic='" + topic + '\'' +
                ", offset=" + offset +
                ", physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", delayLevel=" + delayLevel +
                ", msgId='" + msgId + '\'' +
                ", autoResend=" + autoResend +
                ", resendCount=" + resendCount +
                ", status=" + status +
                '}';
        }
    }

    public enum ProcessStatus {
        /**
         * In process, the processing result has not yet been returned.
         * */
        RUNNING,

        /**
         * Put message success.
         * */
        SUCCESS,

        /**
         * Put message exception.
         * When autoResend is true, the message will be resend.
         * */
        EXCEPTION,

        /**
         * Skip put message.
         * When the message cannot be looked, the message will be skipped.
         * */
        SKIP,
    }
}
