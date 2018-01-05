package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.update.UpdateFactory;
import org.hobbit.core.rabbit.RabbitMQUtils;

/**
 * OPEN MOCHA Challenge Blazegraph adapter for Task 2
 * 
 * @author f.conrads
 *
 */
public class BlazegraphAdapterTask2 extends AbstractBlazegraphAdapterTask {

	private static final byte UNKOWN_QUERY_TYPE = (byte) 2;
	private static final byte UPDATE_QUERY_TYPE = (byte) 1;
	private static final byte SPARQL_QUERY_TYPE = (byte) 0;

	private byte getQueryType(String queryStr) {
		try {
			QueryFactory.create(queryStr);
			return SPARQL_QUERY_TYPE;
		} catch (Exception e) {
			try {
				UpdateFactory.create(queryStr);
				return UPDATE_QUERY_TYPE;
			} catch (Exception e1) {
				return UNKOWN_QUERY_TYPE;
			}
		}

	}

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		// check if select or insert
		String queryStr = RabbitMQUtils.readString(data);
		switch (getQueryType(queryStr)) {
		case SPARQL_QUERY_TYPE:
			sparql(taskId, data);
			break;
		case UPDATE_QUERY_TYPE:
			update(taskId, data);
			break;
		case UNKOWN_QUERY_TYPE:
		default:
			LOGGER.error("Unkown Query Type {}", queryStr);
		}

	}

	private void sparql(String taskId, byte[] data) {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		select(data, baos);

		byte[] result = baos.toByteArray();
		try {
			sendResultToEvalStorage(taskId, result);
		} catch (IOException e) {
			LOGGER.error("Could not send results to eval storage", e);
		}
	}

	private void update(String taskId, byte[] data) {
		byte[] result = new byte[0];
		try {
			sendResultToEvalStorage(taskId, result);
		} catch (IOException e) {
			LOGGER.error("Could not send results to eval storage", e);
		}
	}

	@Override
	public void receiveGeneratedData(byte[] data) {
		long time = bulkLoad(data);
		LOGGER.info("Bulk Load took {} ms", time);
	}

}
