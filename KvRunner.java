package com.sijia.clientServer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Starts up a server and some clients, does some key value store operations,
 * and then measures how many operations were completed.
 */
public final class KvRunner {
	private static final Logger logger = Logger.getLogger(KvRunner.class.getName());

	private static long DURATION_SECONDS = 10;
    private static int clientThread = 0;
    private static int serverThread = 0;
	private Server server;
	private ManagedChannel channel;

	public static void main(String[] args) throws Exception {
		 
 		    
		 if (args.length != 3) {
		      System.out.println("Usage: args[0] - client thread number, args[1] - server thread number, args[2] - run time ex: 5 second");
		      System.exit(1);
		  }
		   
		    try {
		    	clientThread = Integer.valueOf(args[0]).intValue();
		    	serverThread = Integer.valueOf(args[1]).intValue();
		    	DURATION_SECONDS = Integer.valueOf(args[2]).intValue();
		      
		    } catch (NumberFormatException e) {
		      e.printStackTrace();
		    }
		    
		KvRunner store = new KvRunner();
		store.startServer(serverThread);
		try {
			store.runClient(clientThread);
		} finally {
			store.stopServer();
		}
	}

	private void runClient(int clientThread) throws InterruptedException {
		if (channel != null) {
			throw new IllegalStateException("Already started");
		}
		channel = ManagedChannelBuilder.forTarget("dns:///localhost:" + server.getPort()).usePlaintext(true).build();
		ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

		try {
			AtomicBoolean done = new AtomicBoolean();
			KvClient client = new KvClient(channel,clientThread);

			logger.info("Starting");
			scheduler.schedule(() -> done.set(true), DURATION_SECONDS, TimeUnit.SECONDS);
			client.doClientWork(done);
			TimeUnit.MILLISECONDS.sleep(100);
			double qps = (double) client.getRpcCount() / DURATION_SECONDS;
//			logger.log(Level.INFO, "Result: Client thread Number-> {0}, Server thread Number-> {1}, Did {0} RPCs/s", clientThread, serverThread, new Object[] { qps });
			System.out.println("Result Report: Client Thread Number-> " + clientThread + " Server Thread Number -> " + 
            serverThread + " Test Running Time -> " + DURATION_SECONDS + "s did " + qps + " RPCs/s");
		} finally {
			scheduler.shutdownNow();
			channel.shutdownNow();
		}
	}

	private void startServer(int serverThread) throws IOException {
		if (server != null) {
			throw new IllegalStateException("Already started");
		}
		server = ServerBuilder.forPort(0).addService(new KvService(serverThread)).build();
		server.start();
	}

	private void stopServer() throws InterruptedException {
		Server s = server;
		if (s == null) {
			throw new IllegalStateException("Already stopped");
		}
		server = null;
		s.shutdown();
		if (s.awaitTermination(1, TimeUnit.SECONDS)) {
			return;
		}
		s.shutdownNow();
		if (s.awaitTermination(1, TimeUnit.SECONDS)) {
			return;
		}
		throw new RuntimeException("Unable to shutdown server");
	}
}