package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateFactory;
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

	@Override
	public void receiveGeneratedData(byte[] arg0) {
		if (dataLoadingFinished == false) {
			ByteBuffer dataBuffer = ByteBuffer.wrap(arg0);
			String fileName = RabbitMQUtils.readString(dataBuffer);

			LOGGER.info("Receiving file: " + fileName);

			// graphUris.add(fileName);

			byte[] content = new byte[dataBuffer.remaining()];
			dataBuffer.get(content, 0, dataBuffer.remaining());

			if (content.length != 0) {
				FileOutputStream fos;
				try {
					if (fileName.contains("/"))
						fileName = fileName.replaceAll("[^/]*[/]", "");
					fos = new FileOutputStream(datasetFolderName + File.separator + fileName);
					fos.write(content);
					fos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (totalReceived.incrementAndGet() == totalSent.get()) {
				allDataReceivedMutex.release();
			}
		} else {
			ByteBuffer buffer = ByteBuffer.wrap(arg0);
			String insertQuery = RabbitMQUtils.readString(buffer);

			UpdateRequest updateRequest = UpdateFactory.create(insertQuery);
			try {
				UpdateExecutionFactory.createRemote(updateRequest, url + "sparql");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void insert(String queryString) {
		// TODO: Virtuoso hack
		queryString = queryString.replaceFirst("INSERT DATA", "INSERT");
		queryString += "WHERE { }\n";

		UpdateRequest updateRequest = UpdateFactory.create(queryString);
		try {
			UpdateExecutionFactory.createRemote(updateRequest, url + "sparql").execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public ByteArrayOutputStream sparql(String taskId, String queryString) {
		Query query = QueryFactory.create(queryString);
		QueryExecution qe = QueryExecutionFactory.createServiceRequest(url + "sparql", query);
		ResultSet results = null;
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

		try {
			results = qe.execSelect();
			ResultSetFormatter.outputAsJSON(outputStream, results);
		} catch (Exception e) {
			LOGGER.info("Problem while executing task " + taskId + ": " + queryString);
			// TODO: fix this hacking
			try {
				outputStream.write(
						"{\"head\":{\"vars\":[\"xxx\"]},\"results\":{\"bindings\":[{\"xxx\":{\"type\":\"literal\",\"value\":\"XXX\"}}]}}"
								.getBytes());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			qe.close();
		}
		return outputStream;
	}

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		ByteBuffer buffer = ByteBuffer.wrap(data);
		String queryString = RabbitMQUtils.readString(buffer);
		long timestamp1 = System.currentTimeMillis();
		// LOGGER.info(taskId);
		if (queryString.contains("INSERT DATA")) {
			insert(queryString);
			try {
				this.sendResultToEvalStorage(taskId, RabbitMQUtils.writeString(""));
			} catch (IOException e) {
				LOGGER.error("Got an exception while sending results.", e);
			}
		} else {
			
			ByteArrayOutputStream outputStream = sparql(taskId, queryString);
			try {
				this.sendResultToEvalStorage(taskId, outputStream.toByteArray());
			} catch (IOException e) {
				LOGGER.error("Got an exception while sending results.", e);
			}
		}
		long timestamp2 = System.currentTimeMillis();
		LOGGER.info("Task " + taskId + ": " + (timestamp2 - timestamp1));
	}

	@Override
	public void receiveCommand(byte command, byte[] data) {

		if (BULK_LOAD_DATA_GEN_FINISHED == command) {

			ByteBuffer buffer = ByteBuffer.wrap(data);
			int numberOfMessages = buffer.getInt();
			boolean lastBulkLoad = buffer.get() != 0;

			LOGGER.info("Bulk loading phase (" + loadingNumber + ") begins");

			// if all data have been received before BULK_LOAD_DATA_GEN_FINISHED command
			// received
			// release before acquire, so it can immediately proceed to bulk loading
			if (totalReceived.get() == totalSent.addAndGet(numberOfMessages)) {
				allDataReceivedMutex.release();
			}

			LOGGER.info("Wait for receiving all data for bulk load " + loadingNumber + ".");
			try {
				allDataReceivedMutex.acquire();
			} catch (InterruptedException e) {
				LOGGER.error(
						"Exception while waitting for all data for bulk load " + loadingNumber + " to be recieved.", e);
			}
			LOGGER.info("All data for bulk load " + loadingNumber + " received. Proceed to the loading...");

			loadDataset("http://graph.version." + loadingNumber);

			try {
				sendToCmdQueue(BULK_LOADING_DATA_FINISHED);
			} catch (IOException e) {
				e.printStackTrace();
			}

			LOGGER.info("Bulk loading phase (" + loadingNumber + ") is over.");

			loadingNumber++;

			if (lastBulkLoad) {
				dataLoadingFinished = true;
				File theDir = new File(datasetFolderName);
				for (File f : theDir.listFiles())
					f.delete();
				LOGGER.info("All bulk loading phases are over.");
			}
		}
		super.receiveCommand(command, data);
	}

	private void loadDataset(String graphURI) {

		try {
			DataLoader.main(new String[] { "-defaultGraph", graphURI, "RWStore.properties", datasetFolderName });
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void init() throws Exception {
		LOGGER.info("Initialization begins.");
		super.init();
		internalInit();
		LOGGER.info("Initialization is over.");
	}

	public void internalInit() {
		datasetFolderName = "./myvol/datasets";
		File theDir = new File(datasetFolderName);
		theDir.mkdir();
	}

}
