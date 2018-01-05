package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.IOException;
import java.util.Iterator;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.core.rabbit.RabbitMQUtils;

public class BlazegraphAdapterTask4 extends AbstractBlazegraphAdapterTask {

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		// receive sparql
		String queryString = RabbitMQUtils.readString(data);
		Query query = QueryFactory.create(queryString);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(url+"sparql", query);
		ResultSet res = qexec.execSelect();
		byte[] result;
		StringBuilder builder= new StringBuilder();
		int size = ResultSetFormatter.consume(res);
		while(res.hasNext()) {
			QuerySolution solution = res.next();
			Iterator<String> varNames = solution.varNames();
			while(varNames.hasNext()) {
				String varName = varNames.next();
				if(solution.get(varName).isURIResource()) {
					builder.append(solution.get(varName).asResource().getURI());
				}
				else {
					//assuming it is a count
					builder.append(solution.get(varName).asLiteral().getLong());
				}
				if(res.getRowNumber()<size) {
					builder.append(", ");
				}
			}
		}
		try {
			sendResultToEvalStorage(taskId, builder.toString().getBytes());
		} catch (IOException e) {
			LOGGER.error("Could not send results to storage.", e);
		}
	}

	@Override
	public void receiveGeneratedData(byte[] data) {
		// TODO Auto-generated method stub

	}

}