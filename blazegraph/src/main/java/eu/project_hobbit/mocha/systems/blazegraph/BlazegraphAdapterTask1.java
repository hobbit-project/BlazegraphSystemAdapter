package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * OPEN MOCHA Challenge Blazegraph adapter for Task 1
 * 
 * @author f.conrads
 *
 */
public class BlazegraphAdapterTask1 extends AbstractBlazegraphAdapterTask{

	
	@Override
	public void receiveGeneratedData(byte[] data) {
		//receive inserts
		long time = insert(data);
		LOGGER.info("Insert took {} ms", time);
	}

	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
		//receive sparql
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		long time = select(data, baos);
		LOGGER.info("Query took {} ms", time);
		byte[] result = baos.toByteArray();
		try {
			sendResultToEvalStorage(taskId, result);
		} catch (IOException e) {
			LOGGER.error("Could not send results to storage.", e);
		}
	}
	
	
	


}
