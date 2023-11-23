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
package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {

    // 对象引用计数，初始值为 1，创建时就被引用了，直到销毁
    // MappedFile 就是通过它来管理资源引用，判断是否可以被销毁。
    protected final AtomicLong refCount = new AtomicLong(1);
    protected volatile boolean available = true;
    protected volatile boolean cleanupOver = false;
    private volatile long firstShutdownTimestamp = 0;

    //MappedFile 在任何使用的地方，都会先通过 hold() 持有一个引用计数，使用完了之后就会调用 release() 释放引用计数
    public synchronized boolean hold() {
        //是否可用，只有在shutdown时候才会为false
        if (this.isAvailable()) {
            //是否在引用中，并 +1
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    //mappedFile的销毁
    public void shutdown(final long intervalForcibly) {
        //如果可用
        if (this.available) {
            //置为不可用
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            //释放资源
            this.release();
        } else if (this.getRefCount() > 0) {
            //如果引用不为0
            //距离第一次关闭是否超过  120s
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
