package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.store.DataLoader;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateRequest;

/**
 * Abstract Blazegraph adapter (will initiate start and stop of server)
 * 
 * @author f.conrads
 *
 */
public abstract class AbstractBlazegraphAdapterTask extends AbstractSystemAdapter {

	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBlazegraphAdapterTask.class);

	protected String url = "http://0.0.0.0:15151/blazegraph/sparql";
	protected static Process serverProcess;


	
	@Override
	public void init() throws Exception {
		super.init();
		
		startServer();
		boolean waited = serverProcess.waitFor(10, TimeUnit.SECONDS);
		System.out.println("Server terminated? "+waited);
	}

	protected boolean pingServer(int timeout) {
	    try (Socket socket = new Socket()) {
	        socket.connect(new InetSocketAddress("0.0.0.0", 15151), timeout);
	        return true;
	    } catch (IOException e) {
	        return false; // Either timeout or unreachable or failed DNS lookup.
	    }
	}
	
	/**
	 * Starts the NanoSparqlServer as a Thread
	 * 
	 * @throws Exception
	 */
	public void startServer() throws Exception {
		// Start blazegraph

		if(pingServer(10000)) {
			System.out.println("Server instance already started");
			return;
		}
		File jar = new File("repository/blazegraph.jar");
		LOGGER.info("Start blazegraph server.");
		ProcessBuilder b = new ProcessBuilder(new String[] {"java", "-server", "-Djetty.port=15151", "-Xmx4G", "-jar", jar.getAbsolutePath(), "RWStore.properties"});
		b.directory(new File("."));
		serverProcess = b.start();
		InputStream is = serverProcess.getInputStream();
	     InputStreamReader isr = new InputStreamReader(is);
	     BufferedReader br = new BufferedReader(isr);
	     String line;
	 
	     System.out.printf("Output of running %s is:", 
	        Arrays.toString(b.command().toArray()));
	 
	     while ((line = br.readLine()) != null) {
	       if(line.contains("Address already in use")) {
	    	   System.out.println("Server could not start");
	    	   System.out.println(line);
	    	   break;
	       }
	       else if(line.contains("serviceURL:")) {
	    	   System.out.println("Server could start");
	    	   System.out.println(line);
	    	   break;
	       }
	     }
	    
//	    serverProcess = Runtime.getRuntime().exec(jar.getAbsolutePath());
	   
	}

 
	protected long bulkLoad(byte[] data) {
		return bulkLoad(data, "http://default.com");
	}

	protected long bulkLoad(byte[] data, String graph) {
		LOGGER.info("Starting bulk load.");
		long bulkLoadTime = -1;
		// bulk load received data
		String fileName = UUID.randomUUID().toString() + ".ttl";
		File dir = new File("tmpbla");
		dir.mkdirs();
		File file = new File(dir.getAbsolutePath()+File.separator+fileName);
		LOGGER.error("[Bulk Load] Create file {} and writing data into it", fileName);
		try {
			file.createNewFile();
			try (PrintWriter pw = new PrintWriter(file)) {
				pw.print(RabbitMQUtils.readString(data));
			} catch (FileNotFoundException e1) {
				LOGGER.error("[Bulk Load] Could not find File.", e1);
				e1.printStackTrace();
			}
			closeServer(false);
			System.out.println("BulkLoad");
			LOGGER.info("[Bulk Load] Starting actual bulk load now using graph: {} and file: {}", graph, dir.getAbsolutePath()+File.separator+fileName);
			long bulkLoadStartTime = Calendar.getInstance().getTimeInMillis();
			DataLoader.main(new String[] { "-defaultGraph", graph, "RWStore.properties", dir.getAbsolutePath()+File.separator+fileName });
			long bulkLoadEndTime = Calendar.getInstance().getTimeInMillis();
			startServer();
			bulkLoadTime = bulkLoadEndTime - bulkLoadStartTime;
			LOGGER.info("[Bulk Load] Bulk load took {} ms", bulkLoadTime);
		} catch (IOException e) {
			LOGGER.error("[Bulk Load] Could not bulk load data due to I/O.", e);
			e.printStackTrace();
		} catch (Exception e) {
			LOGGER.error("[Bulk Load] Could not bulk load data due to unkown", e);
				e.printStackTrace();
		} finally {
			LOGGER.info("[Bulk Load] Deleting temp file {}", fileName);
			file.delete();
		}
		return bulkLoadTime;
	}

	@Override
	public void close() throws IOException {
		closeServer();
		super.close();
	}

	/**
	 * Closes the server and deletes the journal file
	 */
	public void closeServer() {
		closeServer(true);
	}
	
	/**
	 * Stops the Server and clean up the db file
	 * @param removeJournal true if journal file should be deleted
	 */
	public void closeServer(boolean removeJournal) {
		// stop blazegraph
		
		try {
			System.out.println("Close Server. remove:"+removeJournal);
			LOGGER.info("Close Server [remove journal : {} ]", removeJournal);
//			serverProcess.destroy();
			if(serverProcess!=null) {
				serverProcess.destroyForcibly();
				serverProcess.waitFor();
				// remove blazegraph.jnl
				if(removeJournal) {
					removeJournal();
				}
			}
			
			System.out.println("Closed Server");
			LOGGER.info("Blazegraph stopped.");
		} catch (Exception e) {
			LOGGER.error("Could not force stop Blazegraph.", e);
			e.printStackTrace();
		}
	}
	
	private void removeJournal() {
		File journal = new File("blazegraph.jnl");
		journal.delete();
	}

	protected long select(byte[] data, OutputStream outStream) {
		String queryString = RabbitMQUtils.readString(data);
		LOGGER.info("[SELECT] Got query: {}", queryString.replace("\n", " "));
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(url, query);
		long start = Calendar.getInstance().getTimeInMillis();
		ResultSet res = qexec.execSelect();
		long end = Calendar.getInstance().getTimeInMillis();
		ResultSetFormatter.outputAsJSON(outStream, res);
		LOGGER.info("[SELECT] Query finished.");
		return end - start;
	}

	protected long insert(byte[] data) {
		String insertQuery = RabbitMQUtils.readString(data);
		LOGGER.info("[UPDATE] Got update: {}", insertQuery.replace("\n", " "));
		UpdateRequest update = new UpdateRequest();
		update.add(insertQuery);
		UpdateProcessor processor = UpdateExecutionFactory.createRemote(update, url);
		long start = Calendar.getInstance().getTimeInMillis();
		processor.execute();
		long end = Calendar.getInstance().getTimeInMillis();
		LOGGER.info("[UPDATE] Query finished.");
		return end - start;
	}

}
