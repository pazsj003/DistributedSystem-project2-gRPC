package com.sijia.clientServer;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Status;
import io.grpc.Status.Code;
import com.sijia.clientServer.KvJava.CreateRequest;
import com.sijia.clientServer.KvJava.CreateResponse;
import com.sijia.clientServer.KvJava.DeleteRequest;
import com.sijia.clientServer.KvJava.DeleteResponse;
import com.sijia.clientServer.KvJava.RetrieveRequest;
import com.sijia.clientServer.KvJava.RetrieveResponse;
import com.sijia.clientServer.KvJava.UpdateRequest;
import com.sijia.clientServer.KvJava.UpdateResponse;

import io.grpc.stub.ClientCalls;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

final class KvClient {
	private static final Logger logger = Logger.getLogger(KvClient.class.getName());

	private final int MEAN_KEY_SIZE = 64;
	private final int MEAN_VALUE_SIZE = 65536;

	private final RandomAccessSet<ByteBuffer> knownKeys = new RandomAccessSet<>();
	private final Channel channel;

	private AtomicLong rpcCount = new AtomicLong();
	private AtomicLong sendId = new AtomicLong();

	private final Semaphore limiter; 
	SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss:SSS");

	KvClient(Channel channel, int threadNum) {
		this.channel = channel;
		 
		limiter = new Semaphore(threadNum);
	}

	long getRpcCount() {
		return rpcCount.get();
	}

	long getSendId() {
		return sendId.get();
	}

	/**
	 * Does the client work until {@code done.get()} returns true. Callers should
	 * set done to true, and wait for this method to return.
	 */
	void doClientWork(AtomicBoolean done) throws InterruptedException {
		Random random = new Random();
		AtomicReference<Throwable> errors = new AtomicReference<>();

		while (!done.get() && errors.get() == null) {
			// Pick a random CRUD action to take.
			int command = random.nextInt(4);
			if (command == 0) {
				doPut(channel, errors);
			} else if (command == 1) {
				doGet(channel, errors);
			} else if (command == 2) {
				doUpdate(channel, errors);
			} else if (command == 3) {
				doDelete(channel, errors);
			} else {
				throw new AssertionError();
			}
		}
		if (errors.get() != null) {
			throw new RuntimeException(errors.get());
		}
	}

	/**
	 * PUT a random key and value.
	 */
	private void doPut(Channel chan, AtomicReference<Throwable> error) throws InterruptedException {
		limiter.acquire();
		long localSendId = 0;
		synchronized (sendId) {
			localSendId = sendId.get();
			sendId.incrementAndGet();
		}
        Random rd = new Random();
        ByteBuffer createkey = createRandomKey();
        if (rd.nextInt(10) == 5) {
        	createkey = createRandomKey();
        	synchronized (knownKeys) {
        	createkey = knownKeys.getRandomKey();
        	if (createkey == null) return;
        	}
        } 
        ByteBuffer key = createkey;
		
		
		ClientCall<CreateRequest, CreateResponse> call = chan.newCall(KvJava.CREATE_METHOD, CallOptions.DEFAULT);
		KvJava.CreateRequest req = new KvJava.CreateRequest();
		req.key = key.array();
		byte[] val = randomBytes(MEAN_VALUE_SIZE).array();
		req.value = val;
		long timeCreate = System.currentTimeMillis();
		Date currentTime = new Date(timeCreate);

		System.out.println(
				"Client log: " + sdf.format(currentTime) + " CLIENT_ID: " + localSendId + " SEND: \"PUT\" KEY-> "
						+ req.key[req.key.length - 1] + " VALUE-> " + req.value[req.value.length - 1]);

		ByteBuffer sendId = ByteBuffer.allocate(Long.BYTES);
		sendId.putLong(localSendId);
		req.sendId = sendId.array();

		ListenableFuture<CreateResponse> res = ClientCalls.futureUnaryCall(call, req);
		res.addListener(() -> {

			rpcCount.incrementAndGet();
			limiter.release();
		}, MoreExecutors.directExecutor());

		Futures.addCallback(res, new FutureCallback<CreateResponse>() {
			@Override
			public void onSuccess(CreateResponse result) {

//				synchronized (knownKeys) {
					knownKeys.add(key);
//				}
				long timeCreate2 = System.currentTimeMillis();
				Date currentTime2 = new Date(timeCreate2);
				System.out.println("Client log: " + sdf.format(currentTime2) + " SERVER_ID: "
						+ result.responseId[result.responseId.length - 1] + " -> CLIENT_ID: "
						+ bytetoint(result.sendId[result.sendId.length - 1]) + " RESPONSE: \"PUT\" SUCESS KEY-> "
						+ result.key[result.key.length - 1] + " VALUE-> " + result.value[result.value.length - 1]);
			}

			@Override
			public void onFailure(Throwable t) {
				Status status = Status.fromThrowable(t);
				if (status.getCode() == Code.ALREADY_EXISTS) {
					synchronized (knownKeys) {
						knownKeys.remove(key);
					}
					long timeCreate2 = System.currentTimeMillis();
					Date currentTime2 = new Date(timeCreate2);
					System.out.println("Client log: " + sdf.format(currentTime2)
							+ " Server Throw Exception ->  CLIENT_ID: " + bytetoint(req.sendId[req.sendId.length - 1])
							+ " RESPONSE: \"PUT\" FAIL KEY-> " + req.key[req.key.length - 1] + " ALREADY EXISTS");
					
//					logger.log(Level.INFO, "Key already existed", t);
				} else {
					error.compareAndSet(null, t);
				}
			}
		});
	}

	/**
	 * Get the value of a random key.
	 */
	private void doGet(Channel chan, AtomicReference<Throwable> error) throws InterruptedException {

		long localSendId = 0;
		synchronized (sendId) {
			localSendId = sendId.get();
			sendId.incrementAndGet();
		}

		ByteBuffer key;
//		synchronized (knownKeys) {
//			key = knownKeys.getRandomKey();
//		}
		// TEST FAIL
		key = knownKeys.getRandomKey();
		
		
		if (key == null) {
			logger.log(Level.FINE, "Nothing to retrieve, continue with the next random action.");
			return;
		}
 

		limiter.acquire();
		ClientCall<RetrieveRequest, RetrieveResponse> call = chan.newCall(KvJava.RETRIEVE_METHOD, CallOptions.DEFAULT);
		KvJava.RetrieveRequest req = new KvJava.RetrieveRequest();

		req.key = key.array();
		long timeCreate = System.currentTimeMillis();
		Date currentTime = new Date(timeCreate);
		System.out.println("Client log: " + sdf.format(currentTime) + " CLIENT_ID: " + localSendId
				+ " SEND: \"GET\" KEY-> " + req.key[req.key.length - 1]);

		ByteBuffer sendId = ByteBuffer.allocate(Long.BYTES);
		sendId.putLong(localSendId);
		req.sendId = sendId.array();

		ListenableFuture<RetrieveResponse> res = ClientCalls.futureUnaryCall(call, req);
		res.addListener(() -> {
			rpcCount.incrementAndGet();
			limiter.release();
		}, MoreExecutors.directExecutor());
		Futures.addCallback(res, new FutureCallback<RetrieveResponse>() {
			@Override
			public void onSuccess(RetrieveResponse result) {

				if (result.value.length < 1) {
					error.compareAndSet(null, new RuntimeException("Invalid response"));
				} else {
					long timeCreate2 = System.currentTimeMillis();
					Date currentTime2 = new Date(timeCreate2);
					System.out.println("Client log: " + sdf.format(currentTime2) + " SERVER_ID: "
							+ result.responseId[result.responseId.length - 1] + " -> CLIENT_ID: "
							+ bytetoint(result.sendId[result.sendId.length - 1]) + " RESPONSE: \"GET\" SUCESS KEY-> "
							+ result.key[result.key.length - 1] + " VALUE-> " + result.value[result.value.length - 1]);

				}
			}

			@Override
			public void onFailure(Throwable t) {
				Status status = Status.fromThrowable(t);
				if (status.getCode() == Code.NOT_FOUND) {
//					synchronized (knownKeys) {
						knownKeys.remove(key);
//					}
//					logger.log(Level.INFO, "Key not found", t);
						long timeCreate2 = System.currentTimeMillis();
						Date currentTime2 = new Date(timeCreate2);
						System.out.println("Client log: " + sdf.format(currentTime2)
								+ " Server Throw Exception ->  CLIENT_ID: " + bytetoint(req.sendId[req.sendId.length - 1])
								+ " RESPONSE: \"GET\" FAIL KEY-> " + req.key[req.key.length - 1] + " NOT FOUND");	
				} else {
					error.compareAndSet(null, t);
				}
			}
		});
	}

	/**
	 * Updates a random key with a random value.
	 */
	private void doUpdate(Channel chan, AtomicReference<Throwable> error) throws InterruptedException {
		long localSendId = 0;
		synchronized (sendId) {
			localSendId = sendId.get();
			sendId.incrementAndGet();
		}

		ByteBuffer key;
		synchronized (knownKeys) {
			key = knownKeys.getRandomKey();
		}
		if (key == null) {
			logger.log(Level.FINE, "Nothing to update, continue with the next random action.");
			return;
		}

		limiter.acquire();
		ClientCall<UpdateRequest, UpdateResponse> call = channel.newCall(KvJava.UPDATE_METHOD, CallOptions.DEFAULT);
		KvJava.UpdateRequest req = new KvJava.UpdateRequest();
		req.key = key.array();
		req.value = randomBytes(MEAN_VALUE_SIZE).array();

		long timeCreate = System.currentTimeMillis();
		Date currentTime = new Date(timeCreate);

		System.out.println(
				"Client log: " + sdf.format(currentTime) + " CLIENT_ID: " + localSendId + " SEND: \"UPDATE\" KEY-> "
						+ req.key[req.key.length - 1] + " VALUE-> " + req.value[req.value.length - 1]);

		ByteBuffer sendId = ByteBuffer.allocate(Long.BYTES);
		sendId.putLong(localSendId);
		req.sendId = sendId.array();

		ListenableFuture<UpdateResponse> res = ClientCalls.futureUnaryCall(call, req);
		res.addListener(() -> {
			rpcCount.incrementAndGet();
			limiter.release();
		}, MoreExecutors.directExecutor());
		Futures.addCallback(res, new FutureCallback<UpdateResponse>() {
			@Override
			public void onSuccess(UpdateResponse result) {
				long timeCreate2 = System.currentTimeMillis();
				Date currentTime2 = new Date(timeCreate2);
				System.out.println("Client log: " + sdf.format(currentTime2) + " SERVER_ID: "
						+ result.responseId[result.responseId.length - 1] + " -> CLIENT_ID: "
						+ bytetoint(result.sendId[result.sendId.length - 1]) + " RESPONSE: \"UPDATE\" SUCESS KEY-> "
						+ result.key[result.key.length - 1] + " VALUE-> " + result.value[result.value.length - 1]);

			}

			@Override
			public void onFailure(Throwable t) {
				Status status = Status.fromThrowable(t);
				if (status.getCode() == Code.NOT_FOUND) {
					synchronized (knownKeys) {
						knownKeys.remove(key);
					}
//          logger.log(Level.INFO, "Key not found", t);
					long timeCreate2 = System.currentTimeMillis();
					Date currentTime2 = new Date(timeCreate2);
					System.out.println("Client log: " + sdf.format(currentTime2)
							+ " Server Throw Exception ->  CLIENT_ID: " + bytetoint(req.sendId[req.sendId.length - 1])
							+ " RESPONSE: \"UPDATE\" FAIL KEY-> " + req.key[req.key.length - 1] + " NOT FOUND");

				} else {
					error.compareAndSet(null, t);
				}
			}
		});
	}

	/**
	 * Deletes the value of a random key.
	 */
	private void doDelete(Channel chan, AtomicReference<Throwable> error) throws InterruptedException {

		long localSendId = 0;
		synchronized (sendId) {
			localSendId = sendId.get();
			sendId.incrementAndGet();
		}

		ByteBuffer key;

//		synchronized (knownKeys) {
//			key = knownKeys.getRandomKey();
//			if (key != null) {
//				knownKeys.remove(key);
//			}
//		}
		
	     // for test FAIL 
			key = knownKeys.getRandomKey();
			if (key != null) {
				knownKeys.remove(key);
			}
	
		
		if (key == null) { // log outside of a synchronized block
			logger.log(Level.FINE, "Nothing to delete, continue with the next random action.");

			return;
		}

		limiter.acquire();
		ClientCall<DeleteRequest, DeleteResponse> call = chan.newCall(KvJava.DELETE_METHOD, CallOptions.DEFAULT);
		DeleteRequest req = new DeleteRequest();
		req.key = key.array();

		long timeCreate = System.currentTimeMillis();
		Date currentTime = new Date(timeCreate);

		System.out.println("Client log: " + sdf.format(currentTime) + " CLIENT_ID: " + localSendId
				+ " SEND: \"DELETE\" KEY-> " + req.key[req.key.length - 1]);

		ByteBuffer sendId = ByteBuffer.allocate(Long.BYTES);
		sendId.putLong(localSendId);
		req.sendId = sendId.array();

		ListenableFuture<DeleteResponse> res = ClientCalls.futureUnaryCall(call, req);
		res.addListener(() -> {
			rpcCount.incrementAndGet();
			limiter.release();
		}, MoreExecutors.directExecutor());
		Futures.addCallback(res, new FutureCallback<DeleteResponse>() {
			@Override
			public void onSuccess(DeleteResponse result) {
				long timeCreate2 = System.currentTimeMillis();
				Date currentTime2 = new Date(timeCreate2);
				System.out.println("Client log: " + sdf.format(currentTime2) + " SERVER_ID: "
						+ result.responseId[result.responseId.length - 1] + " -> CLIENT_ID: "
						+ bytetoint(result.sendId[result.sendId.length - 1]) + " RESPONSE: \"DELETE\" SUCESS KEY-> "
						+ result.key[result.key.length - 1]);
			}

			@Override
			public void onFailure(Throwable t) {
				Status status = Status.fromThrowable(t);
				if (status.getCode() == Code.NOT_FOUND) {
					long timeCreate2 = System.currentTimeMillis();
					Date currentTime2 = new Date(timeCreate2);
					System.out.println("Client log: " + sdf.format(currentTime2)
							+ " Server Throw Exception ->  CLIENT_ID: " + bytetoint(req.sendId[req.sendId.length - 1])
							+ " RESPONSE: \"DELETE\" FAIL KEY-> " + req.key[req.key.length - 1] + " NOT FOUND");
					
//					logger.log(Level.INFO, "Key not found", t);
				} else {
					error.compareAndSet(null, t);
				}
			}
		});
	}

	/**
	 * Creates and adds a key to the set of known keys.
	 */
	private ByteBuffer createRandomKey() {
		ByteBuffer key;
		do {
			key = randomBytes(MEAN_KEY_SIZE);

		} while (knownKeys.contains(key));
		return key;
	}

	/**
	 * Creates an exponentially sized byte string with a mean size.
	 */

	private static ByteBuffer randomBytes(int mean) {
		Random random = new Random();
		// An exponentially distributed random number.
		int size = (int) Math.round(mean * -Math.log(1 - random.nextDouble()));
		byte[] bytes = new byte[1 + mean];
		random.nextBytes(bytes);
		return ByteBuffer.wrap(bytes);
	}

	public int bytetoint(byte tb) {
		int temp;
		temp = Integer.valueOf(tb);
		if (temp < 0) {
			temp = temp & 0x7F + 128;
		}
		return temp;
	}

}