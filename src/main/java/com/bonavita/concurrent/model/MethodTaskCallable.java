/**
 * 
 */
package com.bonavita.concurrent.model;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * @author mukesh
 *
 */
public class MethodTaskCallable<T> implements Callable<T> {
	private static final Logger LOG = LoggerFactory.getLogger(MethodTaskCallable.class);
	private static Map<String, Class<?>> primitiveToWrapperMap;
	
	static {
		primitiveToWrapperMap = new HashMap<>();
		primitiveToWrapperMap.put(Short.TYPE.getName(), Short.class);
		primitiveToWrapperMap.put(Integer.TYPE.getName(), Integer.class);
		primitiveToWrapperMap.put(Long.TYPE.getName(), Long.class);
		primitiveToWrapperMap.put(Float.TYPE.getName(), Float.class);
		primitiveToWrapperMap.put(Double.TYPE.getName(), Double.class);
		primitiveToWrapperMap.put(Boolean.TYPE.getName(), Boolean.class);
		primitiveToWrapperMap.put(Character.TYPE.getName(), Character.class);
		primitiveToWrapperMap.put(Byte.TYPE.getName(), Byte.class);
		primitiveToWrapperMap.put(Void.TYPE.getName(), Void.class);
	}
	
	private String methodName;
	private Object service;
	private Object[] args;
	private T result;
	private boolean isSuccessful;
	private Exception exception;
	private Map<String, String> previousContext;
	
	public MethodTaskCallable(String methodName, Object service, Object... args) {
		this.methodName = methodName;
		this.service = service;
		this.args = args;
		this.previousContext = MDC.getCopyOfContextMap();
	}

	@Override
	public T call() throws Exception {
	  if(previousContext != null) {
	    MDC.setContextMap(previousContext);
	  }
		T result = null;
		try {
			Method method = getMethod(methodName, service, args);
			if(method == null) {
			  LOG.error("no method found with name {}", methodName);
			} else {
    			Object[] formatArgs = formatArgs(method, args);
    			Object response = method.invoke(service, formatArgs);
    			if(response != null) {
    				result = (T) response;
    			}
    			this.isSuccessful = true;
			}
			
		} catch(Exception e) {
			LOG.error("error executing method " + methodName, e);
			this.exception = e;
			throw e;
		} finally {
		  MDC.clear();
		}
		return result;
	}

	public T getResult() {
		return result;
	}

	public boolean isSuccessful() {
		return isSuccessful;
	}

	public Exception getException() {
		return exception;
	}
	
	private Method getMethod(String methodName, Object service, Object... args) throws NoSuchMethodException {
		Method method = null;
		Method[] serviceMethods = service.getClass().getMethods();
		for(Method m: serviceMethods) {
			if(m.getName().equals(methodName)) {
				if(isMatchArgs(m, args)) {
					method = m;
					method.setAccessible(true);
				}
			}
		}
		if(method == null) throw new NoSuchMethodException(methodName + " with args " + args);
		return method;
	}

	private Object[] formatArgs(Method m, Object... args) {
		Class[] paramTypes = m.getParameterTypes();
		int typeCount = paramTypes.length;
		int argsCount = args.length;
		if(argsCount == typeCount) {
			return args;
		} else if(argsCount > typeCount) {
			int varArgsCount = argsCount - typeCount + 1;
			Object varArgsArray = Array.newInstance(paramTypes[typeCount-1].getComponentType(), varArgsCount);
			for (int j = 0; j < varArgsCount; j++) {
				Array.set(varArgsArray, j, args[j + typeCount - 1]);
			}
			Object[] resultArgs = new Object[typeCount];
			System.arraycopy(args, 0, resultArgs, 0, typeCount - 1);
			resultArgs[typeCount - 1] = varArgsArray;
			return resultArgs;
		} else {
			return null;
		}
	}

	private boolean isMatchArgs(Method m, Object... args) {
		Class[] paramTypes = m.getParameterTypes();
		if(paramTypes.length <= args.length) {
			boolean isVarArgs = m.isVarArgs();
			if(!isVarArgs && paramTypes.length != args.length)
			  return false;
			for(int i=0; i<paramTypes.length; i++) {
				if(isVarArgs && i == paramTypes.length-1) {
					Class clazz = paramTypes[i].getComponentType();
					for(int j=i; j<args.length; j++) {
						if(args[j] != null && !isInstanceOf(clazz, args[j])) {
							return false;
						}
					}
				} else if(args[i] != null && !isInstanceOf(paramTypes[i], args[i])) {
					return false;
				}
			}
		} else {
			return false;
		}
		return true;
	}

	private boolean isInstanceOf(Class clazz, Object arg) {
		Class wrapper = primitiveToWrapperMap.get(clazz.getName());
		if(wrapper != null) {
			//argument is primitive
			return wrapper.isInstance(arg);
		} else {
			return clazz.isInstance(arg);
		}
	}
}
