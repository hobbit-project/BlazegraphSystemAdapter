package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.sail.webapp.ConfigParams;
import com.bigdata.rdf.sail.webapp.NanoSparqlServer;
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
import org.eclipse.jetty.server.Server;

/**
 * Abstract Blazegraph adapter (will initiate start and stop of server)
 * 
 * @author f.conrads
 *
 */
public abstract class AbstractBlazegraphAdapterTask extends AbstractSystemAdapter {

	private ExecutorService storeExecutor;
	protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractBlazegraphAdapterTask.class);

	private Server server;
	protected String url = "http://localhost:9999/blazegraph/";

	@Override
	public void init() throws Exception {
		super.init();
		// Start blazegraph
		storeExecutor = Executors.newSingleThreadExecutor();
		Map<String, String> initParams = createParams();
		String jettyXml = ClassLoader.getSystemResource("jetty.xml").toURI().toString();
		final Server server = NanoSparqlServer.newInstance(9999, jettyXml, null/* indexManager */, initParams);
		this.server = server;
		storeExecutor.execute(new Runnable() {

			@Override
			public void run() {
				try {
					server.start();
				} catch (Exception e) {
					LOGGER.error("Could not start Blazegraph.", e);
				}
			}
		});

	}

	private Map<String, String> createParams() {
		final Map<String, String> initParams = new LinkedHashMap<String, String>();
		
		initParams.put(ConfigParams.NAMESPACE, "kb");

		initParams.put(ConfigParams.QUERY_THREAD_POOL_SIZE,
				Integer.toString(ConfigParams.DEFAULT_QUERY_THREAD_POOL_SIZE));

		initParams.put(ConfigParams.FORCE_OVERFLOW, Boolean.toString(false));

		return initParams;
	}

	protected long bulkLoad(byte[] data) {
		return bulkLoad(data, "http://default.com");
	}
	
	protected long bulkLoad(byte[] data, String graph) {
		
		long bulkLoadTime=-1;
		// bulk load received data
		String fileName = UUID.randomUUID().toString()+".ttl";
		File file = new File(fileName);
		try {
			file.createNewFile();
			try(PrintWriter pw = new PrintWriter(fileName)){
				pw.print(RabbitMQUtils.readString(data));
			} catch (FileNotFoundException e1) {
				LOGGER.error("Could not find File.", e1);
			}
			long bulkLoadStartTime = Calendar.getInstance().getTimeInMillis();
			DataLoader.main(new String[] {"-defaultGraph", graph, fileName});
			long bulkLoadEndTime = Calendar.getInstance().getTimeInMillis();
			bulkLoadTime = bulkLoadEndTime - bulkLoadStartTime;
		} catch (IOException e) {
			LOGGER.error("Could not bulk load data.", e);
		}
		file.delete();
		return bulkLoadTime;
	}

	@Override
	public void close() throws IOException {
		// stop blazegraph
		storeExecutor.shutdown();
		// Force termination after 5 seconds.
		try {
			server.stop();
			//remove blazegraph.jnl
			File journal =  new File("blazegraph.jnl");
			journal.delete();
			storeExecutor.awaitTermination(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			LOGGER.error("Could not force stop Blazegraph.", e);
		}
		super.close();
	}

	protected long select(byte[] data, OutputStream outStream) {
		String queryString = RabbitMQUtils.readString(data);
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(url+"sparql", query);
		long start = Calendar.getInstance().getTimeInMillis();
		ResultSet res = qexec.execSelect();
		long end = Calendar.getInstance().getTimeInMillis();
		ResultSetFormatter.outputAsJSON(outStream, res);
		return end-start;
	}
	
	protected long insert(byte[] data) {
		String insertQuery = RabbitMQUtils.readString(data);
		UpdateRequest update = new UpdateRequest();
		update.add(insertQuery);
		UpdateProcessor processor = UpdateExecutionFactory.createRemote(update, url+"update");
		long start = Calendar.getInstance().getTimeInMillis();
		processor.execute();
		long end = Calendar.getInstance().getTimeInMillis();
		return end-start;
	}
	
}
