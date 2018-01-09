package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.hobbit.core.rabbit.RabbitMQUtils;

/**
 * OPEN MOCHA Challenge Blazegraph adapter for Task 3
 * 
 * @author f.conrads
 *
 */
public class BlazegraphAdapterTask3 extends AbstractBlazegraphAdapterTask {

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
		byte[] lengthNameArr = Arrays.copyOfRange(data, 0, 4);
		int lengthName = ByteBuffer.wrap(lengthNameArr).getInt();
		byte[] nameArr = Arrays.copyOfRange(data, 4, 4+lengthName);
		String name = RabbitMQUtils.readString(nameArr);
		byte[] lengthContentArr = Arrays.copyOfRange(data, lengthName + 4, lengthName + 8);
		int lengthContent = ByteBuffer.wrap(lengthContentArr).getInt();
		byte[] content = Arrays.copyOfRange(data, lengthName + 8, lengthName + 8 + lengthContent);
		bulkLoad(content, name);

	}

}
