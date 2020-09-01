package com.sijia.clientServer;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

final class KvService extends KvJava.KeyValueServiceImplBase {

	private static final long READ_DELAY_MILLIS = 10;
	private static final long WRITE_DELAY_MILLIS = 50;
	private final Semaphore limiter; 
	private final ConcurrentMap<ByteBuffer, ByteBuffer> store = new ConcurrentHashMap<>();
	SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss:SSS");
	 
    public KvService (int serverThread) {
    	super();
    	limiter = new Semaphore(serverThread);
    }
	@Override
	public void put(KvJava.CreateRequest request, StreamObserver<KvJava.CreateResponse> responseObserver) {
		try {
			limiter.acquire();
			int threadId = (int) Thread.currentThread().getId();
			ByteBuffer key = ByteBuffer.wrap(request.key);
			ByteBuffer value = ByteBuffer.wrap(request.value);
			ByteBuffer sendId = ByteBuffer.wrap(request.sendId);
			ByteBuffer responseIdId = ByteBuffer.allocate(Integer.BYTES);
			responseIdId.putInt(threadId);
			long timeCreate1 = System.currentTimeMillis();
			Date currentTime1 = new Date(timeCreate1);
			System.out.println("Server log: " + sdf.format(currentTime1) + " CLIENT_ID: "
					+ bytetoint(request.sendId[request.sendId.length - 1]) + " -> SERVER_ID: " + threadId
					+ " RECEIVE: \"PUT\" KEY-> " + request.key[request.key.length - 1] + " VALUE-> "
					+ request.value[request.value.length - 1]);
			simulateWork(WRITE_DELAY_MILLIS);
			if (store.putIfAbsent(key, value) == null) {
				KvJava.CreateResponse response = new KvJava.CreateResponse();
				response.key = key.array();
				response.value = value.array();
				response.sendId = sendId.array();
				response.responseId = responseIdId.array();

				responseObserver.onNext(response);
				long timeCreate2 = System.currentTimeMillis();
				Date currentTime2 = new Date(timeCreate2);
				System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId
						+ " -> CLIENT_ID: " + bytetoint(request.sendId[request.sendId.length - 1])
						+ " SEND: \"PUT\" KEY-> " + request.key[request.key.length - 1] + " VALUE-> "
						+ request.value[request.value.length - 1]);
				responseObserver.onCompleted();

				limiter.release();
				return;
			}
			long timeCreate2 = System.currentTimeMillis();
			Date currentTime2 = new Date(timeCreate2);
			System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId
					+ " -> CLIENT_ID: " + bytetoint(request.sendId[request.sendId.length - 1])
					+ " Throw Exception: \"PUT\" FAIL KEY-> " + request.key[request.key.length - 1] + " ALREADY EXISTS");
			
			limiter.release();
			
			responseObserver.onError(Status.ALREADY_EXISTS.asRuntimeException());
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void get(KvJava.RetrieveRequest request, StreamObserver<KvJava.RetrieveResponse> responseObserver) {
		try {

			limiter.acquire();
			int threadId = (int) Thread.currentThread().getId();
			ByteBuffer key = ByteBuffer.wrap(request.key);

			ByteBuffer sendId = ByteBuffer.wrap(request.sendId);
			ByteBuffer responseIdId = ByteBuffer.allocate(Integer.BYTES);
			responseIdId.putInt(threadId);
			long timeCreate1 = System.currentTimeMillis();
			Date currentTime1 = new Date(timeCreate1);
			System.out.println("Server log: " + sdf.format(currentTime1) + " CLIENT_ID: "
					+ bytetoint(request.sendId[request.sendId.length - 1]) + " -> SERVER_ID: " + threadId
					+ " RECEIVE: \"GET\" KEY-> " + request.key[request.key.length - 1]);

			simulateWork(READ_DELAY_MILLIS);
			ByteBuffer value = store.get(key);

			if (value != null) {
				KvJava.RetrieveResponse response = new KvJava.RetrieveResponse();
				response.key = key.array();
				response.value = value.array();

				response.sendId = sendId.array();

				long timeCreate2 = System.currentTimeMillis();
				Date currentTime2 = new Date(timeCreate2);
				System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId
						+ " -> CLIENT_ID: " + bytetoint(request.sendId[request.sendId.length - 1])
						+ " SEND: \"GET\" KEY-> " + request.key[request.key.length - 1] + " VALUE-> "
						+ response.value[response.value.length - 1]);

				response.responseId = responseIdId.array();

				responseObserver.onNext(response);
				responseObserver.onCompleted();
				limiter.release();
				return;
			}
			long timeCreate2 = System.currentTimeMillis();
			Date currentTime2 = new Date(timeCreate2);
			System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId
					+ " -> CLIENT_ID: " + bytetoint(request.sendId[request.sendId.length - 1])
					+ " Throw Exception: \"GET\" FAIL KEY-> " + request.key[request.key.length - 1] + " NOT FOUND");
			
			limiter.release();
			
			responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void update(KvJava.UpdateRequest request, StreamObserver<KvJava.UpdateResponse> responseObserver) {
		try {
			limiter.acquire();
			int threadId = (int) Thread.currentThread().getId();
			ByteBuffer key = ByteBuffer.wrap(request.key);
			ByteBuffer newValue = ByteBuffer.wrap(request.value);
			ByteBuffer sendId = ByteBuffer.wrap(request.sendId);
			ByteBuffer responseIdId = ByteBuffer.allocate(Integer.BYTES);
			responseIdId.putInt(threadId);
			long timeCreate1 = System.currentTimeMillis();
			Date currentTime1 = new Date(timeCreate1);

			System.out.println("Server log: " + sdf.format(currentTime1) + " CLIENT_ID: "
					+ bytetoint(request.sendId[request.sendId.length - 1]) + " -> SERVER_ID: " + threadId
					+ " RECEIVE: \"UPDATE\" KEY-> " + request.key[request.key.length - 1]);

			simulateWork(WRITE_DELAY_MILLIS);
			ByteBuffer oldValue;
			do {
				oldValue = store.get(key);
				if (oldValue == null) {
					long timeCreate2 = System.currentTimeMillis();
					Date currentTime2 = new Date(timeCreate2);
					System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId
							+ " -> CLIENT_ID: " + bytetoint(request.sendId[request.sendId.length - 1])
							+ " Throw Exception: \"UPDATE\" FAIL KEY-> " + request.key[request.key.length - 1] + " NOT FOUND");
					
					responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
					limiter.release();
					return;
				}
			} while (!store.replace(key, oldValue, newValue));
			KvJava.UpdateResponse response = new KvJava.UpdateResponse();
			response.key = key.array();
			response.value = newValue.array();
			response.sendId = sendId.array();
			response.responseId = responseIdId.array();
			responseObserver.onNext(response);

			long timeCreate2 = System.currentTimeMillis();
			Date currentTime2 = new Date(timeCreate2);
			System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId + " -> CLIENT_ID: "
					+ bytetoint(request.sendId[request.sendId.length - 1]) + " SEND: \"UPDATE\" KEY-> "
					+ request.key[request.key.length - 1] + " VALUE-> " + response.value[response.value.length - 1]);

			responseObserver.onCompleted();
			limiter.release();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void delete(

			KvJava.DeleteRequest request, StreamObserver<KvJava.DeleteResponse> responseObserver) {
		try {
			limiter.acquire();

			int threadId = (int) Thread.currentThread().getId();
			ByteBuffer key = ByteBuffer.wrap(request.key);

			ByteBuffer sendId = ByteBuffer.wrap(request.sendId);
			ByteBuffer responseIdId = ByteBuffer.allocate(Integer.BYTES);
			responseIdId.putInt(threadId);
			long timeCreate1 = System.currentTimeMillis();
			Date currentTime1 = new Date(timeCreate1);

			System.out.println("Server log: " + sdf.format(currentTime1) + " CLIENT_ID: "
					+ bytetoint(request.sendId[request.sendId.length - 1]) + " -> SERVER_ID: " + threadId
					+ " RECEIVE: \"DELETE\" KEY-> " + request.key[request.key.length - 1]);

			simulateWork(WRITE_DELAY_MILLIS);
			if (store.remove(key) != null) {
				KvJava.DeleteResponse response = new KvJava.DeleteResponse();
				response.key = key.array();

				response.sendId = sendId.array();
				response.responseId = responseIdId.array();
				responseObserver.onNext(response);

				long timeCreate2 = System.currentTimeMillis();
				Date currentTime2 = new Date(timeCreate2);
				System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId
						+ " -> CLIENT_ID: " + bytetoint(request.sendId[request.sendId.length - 1])
						+ " SEND: \"DELETE\" KEY-> " + request.key[request.key.length - 1]);

				responseObserver.onCompleted();

				limiter.release();
			} else {
				long timeCreate2 = System.currentTimeMillis();
				Date currentTime2 = new Date(timeCreate2);
				System.out.println("Server log: " + sdf.format(currentTime2) + " SERVER_ID: " + threadId
						+ " -> CLIENT_ID: " + bytetoint(request.sendId[request.sendId.length - 1])
						+ " Throw Exception: \"DELETE\" FAIL KEY-> " + request.key[request.key.length - 1] + " NOT FOUND");
			 
				responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
				limiter.release();
				return;
			}

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int bytetoint(byte tb) {
		int temp;
		temp = Integer.valueOf(tb);
		if (temp < 0) {
			temp = temp & 0x7F + 128;
		}
		return temp;
	}

	private static void simulateWork(long millis) {
		try {
			TimeUnit.MILLISECONDS.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}
}