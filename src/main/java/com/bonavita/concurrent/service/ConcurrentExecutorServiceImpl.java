/**
 * 
 */
package com.bonavita.concurrent.service;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonavita.concurrent.model.MethodTaskCallable;

/**
 * 
 * @author mukesh
 * @param <T>
 *
 */
public class ConcurrentExecutorServiceImpl implements ConcurrentExecutorService {
	private static final Logger LOG = LoggerFactory.getLogger(ConcurrentExecutorServiceImpl.class);
	
	private static final int CORE_POOL_SIZE = 10;
	private static final int MAX_POOL_SIZE = 50;
	private static final int KEEP_ALIVE_SEC = 2 * 60;
	private static final int TIME_OUT_SEC = 10;
	
	private int corePoolSize = CORE_POOL_SIZE;
	private int maximumPoolSize = MAX_POOL_SIZE;
	private int keepAliveSecs = KEEP_ALIVE_SEC;
	private int timeOutSecs = TIME_OUT_SEC;
	
	private ThreadPoolExecutor pool;
	private ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
	
	{
		Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                pool.shutdownNow();
                scheduler.shutdownNow();
            }
        });
		
		scheduler.scheduleWithFixedDelay(new ScheduledTask(), 0l, 60, TimeUnit.SECONDS);
	}
	
	public ConcurrentExecutorServiceImpl() {
		init();
	}
	
	public ConcurrentExecutorServiceImpl(int corePoolSize, int maximumPoolSize, int keepAliveSecs, int timeOutSecs) {
		this.corePoolSize = corePoolSize;
		this.maximumPoolSize = maximumPoolSize;
		this.keepAliveSecs = keepAliveSecs;
		this.timeOutSecs = timeOutSecs;
		init();
	}
	
	@Override
	public <T> void invokeAll(List<MethodTaskCallable<T>>  methods) {
		this.invokeAll(methods, timeOutSecs);
	}
	
	@Override
	public <T> void invokeAll(List<MethodTaskCallable<T>>  methods, int timeOutSecs) {
		try {
			pool.invokeAll(methods, timeOutSecs, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	@Override
	public <T> Future<T> submit(MethodTaskCallable<T> method) {
		return pool.submit(method);
	}
	
	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}

	public void setMaximumPoolSize(int maximumPoolSize) {
		this.maximumPoolSize = maximumPoolSize;
	}

	public void setKeepAliveSecs(int keepAliveSecs) {
		this.keepAliveSecs = keepAliveSecs;
	}

	public void setTimeOutSecs(int timeOutSecs) {
		this.timeOutSecs = timeOutSecs;
	}
	
	private void init() {
		this.pool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 
			keepAliveSecs, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new NamedThreadFactory(ConcurrentExecutorService.class.getSimpleName()), 
			new CustomCallerRunsPolicy());
	}
	
	private class CustomCallerRunsPolicy extends ThreadPoolExecutor.CallerRunsPolicy {
		private AtomicLong count = new AtomicLong(0l);

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            super.rejectedExecution(r, e);
            LOG.info("Total tasks rejected count {}", count.incrementAndGet());
        }
	}
	
	private class NamedThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        NamedThreadFactory(String poolName) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                                  Thread.currentThread().getThreadGroup();
            this.namePrefix = "pool-" + poolName + "-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                                  namePrefix + threadNumber.getAndIncrement(),
                                  0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
	}
	
	private class ScheduledTask implements Runnable {
		
		@Override
		public void run() {
			
			logPoolStats();
			
			int cpsNew = parsePropertyValue("pool.corePoolSize");
			if(isChanged(corePoolSize, cpsNew)) {
				pool.setCorePoolSize(cpsNew);
				LOG.info("corePoolSize changed to {}", cpsNew);
			}
			
			int mpsNew = parsePropertyValue("pool.maxPoolSize");
			if(isChanged(maximumPoolSize, mpsNew)) {
				pool.setMaximumPoolSize(mpsNew);
				LOG.info("maxPoolSize changed to {}", mpsNew);
			}
			
			int kasNew = parsePropertyValue("pool.keepAliveSecs");
			if(isChanged(keepAliveSecs, kasNew)) {
				pool.setKeepAliveTime(kasNew, TimeUnit.SECONDS);
				LOG.info("keepAliveSecs changed to {}", kasNew);
			}
		}
		
		protected int parsePropertyValue(String propertyName) {
			return -1;
		}
		
		private boolean isChanged(int oldVal, int newVal) {
			if(oldVal != newVal && newVal != -1) 
				return true;
			else 
				return false;
		}
		
		private void logPoolStats() {
			LOG.info("total = {}, active = {}, max_reached = {}, done_tasks = {}", 
					pool.getPoolSize(), pool.getActiveCount(), pool.getLargestPoolSize(), 
					pool.getCompletedTaskCount());
	    }
	}
}
