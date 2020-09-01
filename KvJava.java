package com.sijia.clientServer;

import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.MethodDescriptor.MethodType;
import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

final class KvJava {

	private static final String SERVICE_NAME = "io.grpc.KeyValueService";

	static final class CreateRequest implements Serializable {
		private static final long serialVersionUID = 1000;

		byte[] key;
		byte[] value;
		byte[] sendId;

	}

	static final class CreateResponse implements Serializable {
		private static final long serialVersionUID = 2000;
		byte[] key;
		byte[] value;
		byte[] sendId;
		byte[] responseId;

	}

	static final MethodDescriptor<CreateRequest, CreateResponse> CREATE_METHOD = MethodDescriptor
			.newBuilder(marshallerFor(CreateRequest.class), marshallerFor(CreateResponse.class))
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Create"))
			.setType(MethodType.UNARY).setSampledToLocalTracing(true).build();

	static final class RetrieveRequest implements Serializable {
		private static final long serialVersionUID = 3000;

		byte[] key;
		byte[] sendId;

	}

	static final class RetrieveResponse implements Serializable {
		private static final long serialVersionUID = 4000;
		byte[] key;
		byte[] value;
		byte[] sendId;
		byte[] responseId;
	}

	static final MethodDescriptor<RetrieveRequest, RetrieveResponse> RETRIEVE_METHOD = MethodDescriptor
			.newBuilder(marshallerFor(RetrieveRequest.class), marshallerFor(RetrieveResponse.class))
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Retrieve"))
			.setType(MethodType.UNARY).setSampledToLocalTracing(true).build();

	static final class UpdateRequest implements Serializable {
		private static final long serialVersionUID = 5000;

		byte[] key;
		byte[] value;
		byte[] sendId;

	}

	static final class UpdateResponse implements Serializable {
		private static final long serialVersionUID = 6000;
		byte[] key;
		byte[] value;
		byte[] sendId;
		byte[] responseId;
	}

	static final MethodDescriptor<UpdateRequest, UpdateResponse> UPDATE_METHOD = MethodDescriptor
			.newBuilder(marshallerFor(UpdateRequest.class), marshallerFor(UpdateResponse.class))
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Update"))
			.setType(MethodType.UNARY).setSampledToLocalTracing(true).build();

	static final class DeleteRequest implements Serializable {
		private static final long serialVersionUID = 7000;
		byte[] key;
		byte[] value;
		byte[] sendId;
	}

	static final class DeleteResponse implements Serializable {
		private static final long serialVersionUID = 8000;
		byte[] key;
		byte[] sendId;
		byte[] responseId;
	}

	static final MethodDescriptor<DeleteRequest, DeleteResponse> DELETE_METHOD = MethodDescriptor
			.newBuilder(marshallerFor(DeleteRequest.class), marshallerFor(DeleteResponse.class))
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Delete"))
			.setType(MethodType.UNARY).setSampledToLocalTracing(true).build();

	static abstract class KeyValueServiceImplBase implements BindableService {
		public abstract void put(KvJava.CreateRequest request, StreamObserver<CreateResponse> responseObserver);

		public abstract void get(KvJava.RetrieveRequest request,
				StreamObserver<KvJava.RetrieveResponse> responseObserver);

		public abstract void update(KvJava.UpdateRequest request,
				StreamObserver<KvJava.UpdateResponse> responseObserver);

		public abstract void delete(KvJava.DeleteRequest request,
				StreamObserver<KvJava.DeleteResponse> responseObserver);

		@Override
		public final ServerServiceDefinition bindService() {
			ServerServiceDefinition.Builder ssd = ServerServiceDefinition.builder(SERVICE_NAME);
			ssd.addMethod(CREATE_METHOD,
					ServerCalls.asyncUnaryCall((request, responseObserver) -> put(request, responseObserver)));
			ssd.addMethod(RETRIEVE_METHOD,
					ServerCalls.asyncUnaryCall((request, responseObserver) -> get(request, responseObserver)));
			ssd.addMethod(UPDATE_METHOD,
					ServerCalls.asyncUnaryCall((request, responseObserver) -> update(request, responseObserver)));
			ssd.addMethod(DELETE_METHOD,
					ServerCalls.asyncUnaryCall((request, responseObserver) -> delete(request, responseObserver)));
			return ssd.build();
		}
	}

	static <T> Marshaller<T> marshallerFor(Class<T> clz) {
		return new Marshaller<T>() {
			@Override
			public InputStream stream(T value) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
					oos.writeObject(value);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return new ByteArrayInputStream(baos.toByteArray());
			}

			@Override
			public T parse(InputStream stream) {
				try (ObjectInputStream ois = new ObjectInputStream(stream)) {
					return clz.cast(ois.readObject());
				} catch (IOException e) {
					throw new RuntimeException(e);
				} catch (ClassNotFoundException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}
}