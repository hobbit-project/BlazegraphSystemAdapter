package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.UUID;
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

	protected String url = "http://127.0.0.1:9999/blazegraph/";
	private Process serverProcess;

	@Override
	public void init() throws Exception {
		super.init();
		startServer();
	}

	/**
	 * Starts the NanoSparqlServer as a Thread
	 * 
	 * @throws Exception
	 */
	public void startServer() throws Exception {
		// Start blazegraph
		File jar = new File("repository/blazegraph.jar");
		
	    serverProcess = Runtime.getRuntime().exec(new String[] {"java", "-server", "-Xmx4G", "-jar", jar.getAbsolutePath(), "RWStore.properties"});
	    
	    Thread.sleep(5000);
	}


	protected long bulkLoad(byte[] data) {
		return bulkLoad(data, "http://default.com");
	}

	protected long bulkLoad(byte[] data, String graph) {

		long bulkLoadTime = -1;
		// bulk load received data
		String fileName = UUID.randomUUID().toString() + ".ttl";
		File dir = new File("tmpbla");
		dir.mkdirs();
		File file = new File(dir.getAbsolutePath()+File.separator+fileName);
		try {
			file.createNewFile();
			try (PrintWriter pw = new PrintWriter(file)) {
				pw.print(RabbitMQUtils.readString(data));
			} catch (FileNotFoundException e1) {
				LOGGER.error("Could not find File.", e1);
			}
			closeServer(false);
			long bulkLoadStartTime = Calendar.getInstance().getTimeInMillis();
			DataLoader.main(new String[] { "-defaultGraph", graph, "RWStore.properties", dir.getAbsolutePath()+File.separator+fileName });
			long bulkLoadEndTime = Calendar.getInstance().getTimeInMillis();
			startServer();
			bulkLoadTime = bulkLoadEndTime - bulkLoadStartTime;
		} catch (IOException e) {
			LOGGER.error("Could not bulk load data.", e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
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
//			serverProcess.destroy();
			serverProcess.destroyForcibly();
			serverProcess.waitFor();
			// remove blazegraph.jnl
			if(removeJournal) {
				removeJournal();
			}
		} catch (Exception e) {
			LOGGER.error("Could not force stop Blazegraph.", e);
		}
	}
	
	private void removeJournal() {
		File journal = new File("blazegraph.jnl");
		journal.delete();
	}

	protected long select(byte[] data, OutputStream outStream) {
		String queryString = RabbitMQUtils.readString(data);
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(url + "sparql", query);
		long start = Calendar.getInstance().getTimeInMillis();
		ResultSet res = qexec.execSelect();
		long end = Calendar.getInstance().getTimeInMillis();
		ResultSetFormatter.outputAsJSON(outStream, res);
		return end - start;
	}

	protected long insert(byte[] data) {
		String insertQuery = RabbitMQUtils.readString(data);
		UpdateRequest update = new UpdateRequest();
		update.add(insertQuery);
		UpdateProcessor processor = UpdateExecutionFactory.createRemote(update, url + "sparql");
		long start = Calendar.getInstance().getTimeInMillis();
		processor.execute();
		long end = Calendar.getInstance().getTimeInMillis();
		return end - start;
	}

}
