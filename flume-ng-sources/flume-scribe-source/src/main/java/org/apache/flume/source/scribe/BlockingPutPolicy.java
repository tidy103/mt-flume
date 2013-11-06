package org.apache.flume.source.scribe;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class BlockingPutPolicy implements RejectedExecutionHandler {
	public BlockingPutPolicy() {
		
	}
	
	@Override
	public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
		try {
			e.getQueue().put(r);
		} catch (InterruptedException e1) {
			throw new RejectedExecutionException("Put the BlockingQueue get InterruptedException");
		}
	}
}
