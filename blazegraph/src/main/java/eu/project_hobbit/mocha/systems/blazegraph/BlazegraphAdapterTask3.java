package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class BlazegraphAdapterTask3 extends AbstractBlazegraphAdapterTask{



	@Override
	public void receiveGeneratedTask(String taskId, byte[] data) {
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

	@Override
	public void receiveGeneratedData(byte[] data) {
		// TODO Auto-generated method stub
		
	}


}
