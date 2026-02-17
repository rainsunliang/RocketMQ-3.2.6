/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.sysflag.TopicSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;


/**
 * 运行过程中的路由信息，数据只在内存，宕机后数据消失，但是Broker会定期推送最新数据
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-2
 */
public class RouteInfoManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;  //所有的topic,每个topic的broker名(服务器)列表
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;  //根据brokername快速查找BrokerData
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable; //根据集群名字快速查找,broker(服务器)集合
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable; //根据broker地址 快速查找 channel和ha地址
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable; //根据broker地址查找过滤服务器列表


    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
        this.filterServerTable = new HashMap<String, List<String>>(256);
    }


    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }


    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }


    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * @return 如果是slave，则返回master的ha地址
     */
    //往集群里注册brocker(包括各种更新使用了该接口,一个大接口)
    public RegisterBrokerResult registerBroker(//
            final String clusterName,// 1
            final String brokerAddr,// 2
            final String brokerName,// 3
            final long brokerId,// 4
            final String haServerAddr,// 5
            final TopicConfigSerializeWrapper topicConfigWrapper,// 6  //topicconfig的hash表,topicconfig
            final List<String> filterServerList, // 7
            final Channel channel// 8
    ) {
        RegisterBrokerResult result = new RegisterBrokerResult(); //包括ha地址和master地址
        try {
            try {
                this.lock.writeLock().lockInterruptibly();

                // 更新集群信息
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (null == brokerNames) {
                    brokerNames = new HashSet<String>();
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }
                brokerNames.add(brokerName);

                boolean registerFirst = false;

                // 更新主备信息 
                // 有个hash表保存 brokerid->brockeraddr
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    registerFirst = true;
                    brokerData = new BrokerData();
                    brokerData.setBrokerName(brokerName);
                    HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
                    brokerData.setBrokerAddrs(brokerAddrs);

                    this.brokerAddrTable.put(brokerName, brokerData);
                }
                // brokerId -> brokerAddr
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                registerFirst = registerFirst || (null == oldAddr);

                // 更新Topic信息
                if (null != topicConfigWrapper //
                        && MixAll.MASTER_ID == brokerId) { //master节点
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())//
                            || registerFirst) {
                        ConcurrentHashMap<String, TopicConfig> tcTable =
                                topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            for (String topic : tcTable.keySet()) {
                                TopicConfig topicConfig = tcTable.get(topic);
                                this.createAndUpdateQueueData(brokerName, topicConfig);
                            }
                        }
                    }
                }

                // 更新最后变更时间
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr, //
                    new BrokerLiveInfo(//
                        System.currentTimeMillis(), //
                        topicConfigWrapper.getDataVersion(),//
                        channel, //
                        haServerAddr));
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registerd, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                // 更新Filter Server列表
                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    }
                    else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                // 返回值
                if (MixAll.MASTER_ID != brokerId) {
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }


    /**
     * 判断Topic配置信息是否发生变更
     */
    private boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (null == prev || !prev.getDataVersion().equals(dataVersion)) {
            return true;
        }

        return false;
    }


    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return wipeWritePermOfBroker(brokerName);
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }


    private int wipeWritePermOfBroker(final String brokerName) {
        int wipeTopicCnt = 0;
        Iterator<Entry<String, List<QueueData>>> itTopic = this.topicQueueTable.entrySet().iterator();
        while (itTopic.hasNext()) {
            Entry<String, List<QueueData>> entry = itTopic.next();
            List<QueueData> qdList = entry.getValue();

            Iterator<QueueData> it = qdList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    int perm = qd.getPerm();
                    perm &= ~PermName.PERM_WRITE;
                    qd.setPerm(perm);
                    wipeTopicCnt++;
                }
            }
        }

        return wipeTopicCnt;
    }

    //为某个topic 注册新的 broker
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName); //queueData是borker信息
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());

        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataList) {
            queueDataList = new LinkedList<QueueData>(); //该topic的第一个broker
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
            log.info("new topic registerd, {} {}", topicConfig.getTopicName(), queueData);
        }
        else {
            boolean addNewOne = true;

            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {  //已经存在相同borker名字的信息
                    if (qd.equals(queueData)) { //如果只是名字相同,但是里面的内容不同,则表示要更新
                        addNewOne = false;
                    }
                    else {
                        log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd,
                            queueData);
                        it.remove(); //更新的策略是这里删除,后面添加
                    }
                }
            }

            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }
    }


    //取消broker的注册信息
    public void unregisterBroker(//
            final String clusterName,// 1
            final String brokerAddr,// 2
            final String brokerName,// 3
            final long brokerId// 4
    ) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                if (brokerLiveInfo != null) {
                    log.info("unregisterBroker, remove from brokerLiveTable {}, {}", //
                        (brokerLiveInfo != null ? "OK" : "Failed"),//
                        brokerAddr//
                    );
                }

                this.filterServerTable.remove(brokerAddr);

                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}", //
                        (addr != null ? "OK" : "Failed"),//
                        brokerAddr//
                    );

                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}", //
                            brokerName//
                        );

                        removeBrokerName = true;
                    }
                }

                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}", //
                            (removed ? "OK" : "Failed"),//
                            brokerName//
                        );

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}", //
                                clusterName//
                            );
                        }
                    }

                    // 删除相应的topic
                    this.removeTopicByBrokerName(brokerName);
                }
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }


    // 遍历所有topic的borker列表,删除该broker消息,如果某个topic没有broker则,删除topic
    private void removeTopicByBrokerName(final String brokerName) {
        Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, List<QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    it.remove();
                }
            }

            if (queueDataList.isEmpty()) {
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
        }
    }


    //将topic相关的所有路由信息返回: 多个borker集群&每个集群多个broker&filterServerList
    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();
        boolean foundQueueData = false;
        boolean foundBrokerData = false;
        Set<String> brokerNameSet = new HashSet<String>();
        List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                this.lock.readLock().lockInterruptibly();
                List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                if (queueDataList != null) {
                    topicRouteData.setQueueDatas(queueDataList);
                    foundQueueData = true;

                    // BrokerName去重(by Liang: 也就是插入操作没有保证唯一, BrokerName相同则一个集群吧, 然后brokerid不同)
                    // 一个topic有多个broker集群(多个Broker名字(对应一个QueueData)), 每个broker集群(broker名字相同),有多个不同brokerid
                    Iterator<QueueData> it = queueDataList.iterator();
                    while (it.hasNext()) {
                        QueueData qd = it.next();
                        brokerNameSet.add(qd.getBrokerName());
                    }

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            BrokerData brokerDataClone = new BrokerData();
                            brokerDataClone.setBrokerName(brokerData.getBrokerName());
                            brokerDataClone.setBrokerAddrs((HashMap<Long, String>) brokerData
                                .getBrokerAddrs().clone()); //所有同一个broker名字不同brokerid的地址列表
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;

                            // 增加Filter Server
                            for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                filterServerMap.put(brokerAddr, filterServerList);
                            }
                        }
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);
        }

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    // Broker Channel两分钟过期
    private final static long BrokerChannelExpiredTime = 1000 * 60 * 2;


    // 扫描Borker是否过期,过期时间是2分钟如果没有更新LastUpdateTimestamp
    public void scanNotActiveBroker() {
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            if ((last + BrokerChannelExpiredTime) < System.currentTimeMillis()) {
                RemotingUtil.closeChannel(next.getValue().getChannel());
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BrokerChannelExpiredTime);
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
            }
        }
    }


    /**
     * Channel被关闭，或者Channel Idle时间超限
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;

        // 加读锁，寻找断开连接的Broker
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable =
                            this.brokerLiveTable.entrySet().iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                }
                finally {
                    this.lock.readLock().unlock();
                }
            }
            catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        }
        else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        // 加写锁，删除相关数据结构
        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

            try {
                try {
                    this.lock.writeLock().lockInterruptibly();
                    // 清理brokerLiveTable
                    this.brokerLiveTable.remove(brokerAddrFound);

                    // 清理Filter Server
                    this.filterServerTable.remove(brokerAddrFound);

                    // 清理brokerAddrTable
                    String brokerNameFound = null;
                    boolean removeBrokerName = false;
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable =
                            this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();

                        // 遍历Master/Slave，删除brokerAddr
                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                log.info(
                                    "remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                                    brokerId, brokerAddr);
                                break;
                            }
                        }

                        // BrokerName无关联BrokerAddr
                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                                brokerData.getBrokerName());
                        }
                    }

                    // 清理clusterAddrTable
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                log.info(
                                    "remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                                    brokerNameFound, clusterName);

                                // 如果集群对应的所有broker都下线了， 则集群也删除掉
                                if (brokerNames.isEmpty()) {
                                    log.info(
                                        "remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                                        clusterName);
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }

                    // 清理topicQueueTable
                    if (removeBrokerName) {
                        Iterator<Entry<String, List<QueueData>>> itTopicQueueTable =
                                this.topicQueueTable.entrySet().iterator();
                        while (itTopicQueueTable.hasNext()) {
                            Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                            String topic = entry.getKey();
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            while (itQueueData.hasNext()) {
                                QueueData queueData = itQueueData.next();
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    itQueueData.remove();
                                    log.info(
                                        "remove topic[{} {}], from topicQueueTable, because channel destroyed",
                                        topic, queueData);
                                }
                            }

                            if (queueDataList.isEmpty()) {
                                itTopicQueueTable.remove();
                                log.info(
                                    "remove topic[{}] all queue, from topicQueueTable, because channel destroyed",
                                    topic);
                            }
                        }
                    }
                }
                finally {
                    this.lock.writeLock().unlock();
                }
            }
            catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }


    /**
     * 定期打印当前类的数据结构
     */
    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, List<QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, List<QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }


    /**
     * 获取指定集群下的所有 topic 列表
     * 
     * @return
     */
    // 读完代码是返回所有的 集群,每个集群的所有名字,第一个不为空的broker地址！ 不知道这函数干嘛！！！！
    // 从调用方的功能描述是： 获取所有系统内置 Topic 列表
    public byte[] getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (String cluster : clusterAddrTable.keySet()) { //遍历所有集群
                    topicList.getTopicList().add(cluster); //怎么将集群名字加入到 topic列表中？
                    topicList.getTopicList().addAll(this.clusterAddrTable.get(cluster)); //然后将改集群名字所有的broker名加入topic列表
                }

                // 随机取一台 broker (怎么看也不像随机？？？)
                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) { //遍历所有的broker名
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs(); //每个broker名下有多个不同brokerid的broker服务器
                        if (bd.getBrokerAddrs() != null && !bd.getBrokerAddrs().isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next())); //获取第一个不为空的broker地址
                            break;
                        }
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 获取指定集群下的所有 topic 列表
     * 
     * @param cluster
     * @return
     */
    public byte[] getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) { //遍历所有的broker名
                    Iterator<Entry<String, List<QueueData>>> topicTableIt =
                            this.topicQueueTable.entrySet().iterator();
                    while (topicTableIt.hasNext()) { //遍历所有topic
                        Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) { //遍历topic的所有broker名
                            if (brokerName.equals(queueData.getBrokerName())) { //制定的cluster中的所有topic
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 获取单元逻辑下的所有 topic 列表
     * 
     * @return
     */
    // 尚不清楚什么是单元逻辑
    public byte[] getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 获取中心向单元同步的所有 topic 列表
     * 
     * @return
     */
    public byte[] getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) { //跟上函数不同的是调用的函数是:hasUnitSubFlag
                        topicList.getTopicList().add(topic);
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 获取含有单元化订阅组的非单元化 Topic 列表
     * 
     * @return
     */
    public byte[] getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())
                            && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }
}


class BrokerLiveInfo {
    private long lastUpdateTimestamp;
    private DataVersion dataVersion;
    private Channel channel;             //频道信息
    private String haServerAddr;         //ha服务器地址


    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel,
            String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }


    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }


    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }


    public DataVersion getDataVersion() {
        return dataVersion;
    }


    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }


    public Channel getChannel() {
        return channel;
    }


    public void setChannel(Channel channel) {
        this.channel = channel;
    }


    public String getHaServerAddr() {
        return haServerAddr;
    }


    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }


    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
                + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
