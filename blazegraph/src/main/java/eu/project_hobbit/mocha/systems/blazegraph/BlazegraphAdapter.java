package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;
import org.hobbit.core.rabbit.RabbitMQUtils;

import com.bigdata.rdf.store.DataLoader;

/**
 * OPEN MOCHA Challenge Blazegraph adapter for all Task based upon the Virtuoso
 * System Adapter
 * 
 * @author f.conrads
 *
 */
public class BlazegraphAdapter extends AbstractBlazegraphAdapterTask {

	private static final byte BULK_LOAD_DATA_GEN_FINISHED = (byte) 151;
	private static final byte BULK_LOADING_DATA_FINISHED = (byte) 150;
	private boolean dataLoadingFinished = false;
	// SortedSet<String> graphUris = new TreeSet<String>();

	private AtomicInteger totalReceived = new AtomicInteger(0);
	private AtomicInteger totalSent = new AtomicInteger(0);
	private Semaphore allDataReceivedMutex = new Semaphore(0);
	private int loadingNumber = 0;
	private String datasetFolderName;

	public static void main(String args[]) throws Exception {
		BlazegraphAdapter ba = new BlazegraphAdapter();
		ba.startServer();
//		ba.receiveCommand(BULK_LOAD_DATA_GEN_FINISHED, DATA);
		for(int i=1;i<2;i++) {
			System.out.println(ba.sparql("1", "SELECT * {?s ?p ?o} LIMIT 1").toString());
		}
		ba.closeServer();
	}
	
	@Override
	public void receiveGeneratedData(byte[] arg0) {
		if (dataLoadingFinished == false) {
			System.out.println("Bulk Load Size: "+arg0.length);
			saveData(arg0);

			// graphUris.add(fileName);

			if (totalReceived.incrementAndGet() == totalSent.get()) {
				allDataReceivedMutex.release();
			}
		} else {
			insertData(arg0);
		}
	}

	public void insertData(byte[] arg0) {
		byte[] data2 = Arrays.copyOfRange(arg0, 4, arg0.length);
		String insertQuery = RabbitMQUtils.readString(data2);
		System.out.println("[UPDATE] Got update: "+insertQuery.replace("\n", " "));
		UpdateRequest updateRequest = UpdateFactory.create(insertQuery);
		try {
			UpdateProcessor processor = UpdateExecutionFactory.createRemote(updateRequest, url);
			processor.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void saveData(byte[] data) {
		ByteBuffer dataBuffer = ByteBuffer.wrap(data);
		String fileName = RabbitMQUtils.readString(dataBuffer);

		System.out.println("Receiving file: " + fileName);

		// graphUris.add(fileName);

		byte[] content = new byte[dataBuffer.remaining()];
		dataBuffer.get(content, 0, dataBuffer.remaining());

		if (content.length != 0) {
			if (fileName.contains("/"))
				fileName = fileName.replaceAll("[^/]*[/]", "");	
			
			try (PrintWriter pw = new PrintWriter(datasetFolderName + File.separator + fileName + ".ttl")) {
				pw.print(RabbitMQUtils.readString(content));
				pw.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("Receiving file: " + fileName);
	}

	public void insert(String queryString) {
		// TODO: Virtuoso hack
		queryString = queryString.replaceFirst("INSERT DATA", "INSERT");
		queryString += "WHERE { }\n";
		System.out.println("[UPDATE] Got update: "+queryString.replace("\n", " ").substring(0, 40));
//		System.out.println("INSERT "+queryString.replace("\n", " "));
		UpdateRequest updateRequest = UpdateFactory.create(queryString);
		try {
			UpdateProcessor processor = UpdateExecutionFactory.createRemote(updateRequest, url);
			processor.execute();
		} catch (Exception e) {
			System.out.println("[UPDATE] Could not update due to: "+ e);
		}
	}

	public ByteArrayOutputStream sparql(String taskId, String queryString) {
//		System.out.println("[SELECT] Got query: {}", queryString.replace("\n", " "));
		System.out.println("[SELECT] Got query: "+ queryString.replace("\n", " "));
		Query query = QueryFactory.create(queryString);
		QueryExecution qe = QueryExecutionFactory.createServiceRequest(url, query);
		ResultSet results = null;
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
		try {
			results = qe.execSelect();
			ResultSetFormatter.outputAsJSON(outputStream, results);
		} catch (Exception e) {
			System.out.println("SPARQL problem 1 "+e);
			System.out.println("[SELECT] Problem while executing task " + taskId + ": " + queryString);
			// TODO: fix this hacking
			try {
				outputStream.write(
						"{\"head\":{\"vars\":[\"xxx\"]},\"results\":{\"bindings\":[{\"xxx\":{\"type\":\"literal\",\"value\":\"XXX\"}}]}}"
								.getBytes());
			} catch (IOException e1) {
				System.err.println("[SELECT] Could not write JSON due to I/O "+e1);
			}
			System.err.println("[SELECT] Could not write JSON "+e);
		} finally {
			qe.close();
			System.out.println("[SELECT] finish query Execution");
		}
		return outputStream;
	}

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		byte[] data2 = Arrays.copyOfRange(data, 4, data.length);
		String queryString = RabbitMQUtils.readString(data2).trim();
		long timestamp1 = System.currentTimeMillis();
		// System.out.println(taskId);
		System.out.println("[Task] received task "+taskId);
		if (queryString.contains("INSERT DATA")) {
			try {
				insert(queryString);
			}catch(Exception e) {
				System.out.println("[INSERT] problem "+e);
			}
			try {
				this.sendResultToEvalStorage(taskId, RabbitMQUtils.writeString(""));
			} catch (Exception e) {
				System.err.println("[Task] Got an exception while sending results. "+e);
			}
			
		} else {
			byte[] response;
			try {
				ByteArrayOutputStream outputStream = sparql(taskId, queryString);
				response = outputStream.toByteArray();
			} catch(Exception e) {
				System.out.println("SPARQL problem 2 "+e);
				response = new byte[0];
			}
			try {
				this.sendResultToEvalStorage(taskId, response);
			} catch (IOException e) {
				System.err.println("[Task] Got an exception while sending results. "+e);
			} 
		}
		long timestamp2 = System.currentTimeMillis();
		System.out.println("[Task] Task " + taskId + ": " + (timestamp2 - timestamp1));
	}

	@Override
	public void receiveCommand(byte command, byte[] data) {
		boolean started = this.pingServer(10000);
		if(!started) {
			try {
				this.serverProcess.waitFor(10000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if (serverProcess !=null && BULK_LOAD_DATA_GEN_FINISHED == command) {

			ByteBuffer buffer = ByteBuffer.wrap(data);
			int numberOfMessages = buffer.getInt();
			boolean lastBulkLoad = buffer.get() != 0;

			System.out.println("[RecvCommand] Bulk loading phase (" + loadingNumber + ") begins");

			// if all data have been received before BULK_LOAD_DATA_GEN_FINISHED command
			// received
			// release before acquire, so it can immediately proceed to bulk loading
			if (totalReceived.get() == totalSent.addAndGet(numberOfMessages)) {
				allDataReceivedMutex.release();
			}

			System.out.println("[RecvCommand] Wait for receiving all data for bulk load " + loadingNumber + ".");
			try {
				allDataReceivedMutex.acquire();
			} catch (InterruptedException e) {
				System.err.println(
						"[RecvCommand] Exception while waitting for all data for bulk load " + loadingNumber + " to be recieved. "+e);
			}
			System.out.println("[RecvCommand] All data for bulk load " + loadingNumber + " received. Proceed to the loading...");
			this.closeServer(false);
			loadDataset("http://graph.version." + loadingNumber);
			
			

			System.out.println("[RecvCommand] Bulk loading phase (" + loadingNumber + ") is over.");

			loadingNumber++;

			if (lastBulkLoad) {
				dataLoadingFinished = true;
				File theDir = new File(datasetFolderName);
				for (File f : theDir.listFiles())
					f.delete();
				System.out.println("[RecvCommand] All bulk loading phases are over.");
			}
			try {
				this.startServer();
			} catch (Exception e) {
				System.err.println("[RecvCommand] Could not start server "+e);
			}
			try {
				sendToCmdQueue(BULK_LOADING_DATA_FINISHED);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		super.receiveCommand(command, data);
		
	}

	@Override
	public void close() throws IOException {
		closeServer();
		super.close();
	}

	public void loadDataset(String graphURI) {

		try {
			File dir = new File(datasetFolderName);
			DataLoader.main(new String[] { "-verbose", "-defaultGraph", graphURI, "RWStore.properties", dir.getAbsolutePath() });

			FileUtils.deleteDirectory(dir);
			dir.mkdirs();
		} catch (IOException e) {
			System.err.println("[Load Dataset] loading dataset got an exception I/O "+e);
		}
	}

	@Override
	public void init() throws Exception {
		System.out.println("Initialization begins.");
		super.init();
		internalInit();
		System.out.println("Initialization is over.");
	}

	public void internalInit() {
		datasetFolderName = "myvol/datasets";
		File theDir = new File(datasetFolderName);
		try {
			if(theDir.exists())
				FileUtils.deleteDirectory(theDir);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		boolean mkdirs = theDir.mkdirs();
//		System.out.println("[Internal Init] could make dirs? {}", mkdirs);
		System.out.println("[Internal Init] could make dirs? "+mkdirs);
	}

}
