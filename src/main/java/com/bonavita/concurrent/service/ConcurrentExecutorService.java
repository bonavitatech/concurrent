/**
 * 
 */
package com.bonavita.concurrent.service;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.bonavita.concurrent.model.MethodTaskCallable;

/**
 * @author mukesh
 *
 */
public interface ConcurrentExecutorService {
	
	public <T> void invokeAll(List<MethodTaskCallable<T>>  methods);
	
	public <T> void invokeAll(List<MethodTaskCallable<T>>  methods, int timeOutSecs);
	
	public <T> Future<T> submit(MethodTaskCallable<T> method);
	
	public <T> Future<T> submit(Callable<T> callable);
	
}
