package eu.project_hobbit.mocha.systems.blazegraph;

import java.io.ByteArrayOutputStream;

import org.hobbit.core.rabbit.RabbitMQUtils;
import org.junit.Test;

/**
 * Tests all taks of the adapter
 * 
 * @author f.conrads
 *
 */
public class BlazegraphAdapterTest {

	
	private void start(AbstractBlazegraphAdapterTask task) throws Exception {
		task.startServer();
	}
	
	private void stop(AbstractBlazegraphAdapterTask task) throws Exception {
		task.closeServer();
	}
	
	
	/**
	 * Tests the first task of the adapter
	 * @throws Exception 
	 */
	@Test
	public void task1Test() throws Exception {
		BlazegraphAdapterTask1 task = new BlazegraphAdapterTask1();
		start(task);
		byte[] data=null;
		task.receiveGeneratedData(data);

		data=null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		task.select(data, baos);
		System.out.println(RabbitMQUtils.readString(baos.toByteArray()));
		stop(task);
	}

	/**
	 * Tests the second task of the adapter
	 * @throws Exception 
	 */	
	@Test
	public void task2Test() throws Exception {
		BlazegraphAdapterTask2 task = new BlazegraphAdapterTask2();
		start(task);
		byte[] data=null;
		//bulkload
		task.receiveGeneratedData(data);
		//sparql
		data=null;
		data = task.sparql(data);
		System.out.println(RabbitMQUtils.readString(data));
		//update 
		data=null;
		task.update(data);
		data = task.sparql(data);
		System.out.println(RabbitMQUtils.readString(data));
		stop(task);
	}
	
	/**
	 * Tests the third task of the adapter
	 * @throws Exception 
	 */
	@Test
	public void task3Test() throws Exception {
		BlazegraphAdapterTask3 task = new BlazegraphAdapterTask3();
		start(task);
		//data: length:name:length:content
		byte[] data=null;
		task.receiveGeneratedData(data);

		data=null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		task.select(data, baos);
		System.out.println(RabbitMQUtils.readString(baos.toByteArray()));
		stop(task);
	}
	
	/**
	 * Tests the fourth task of the adapter
	 * @throws Exception 
	 */
	@Test
	public void task4Test() throws Exception {
		BlazegraphAdapterTask4 task = new BlazegraphAdapterTask4();
		start(task);
		//bulk load
		byte[] data=null;
		task.receiveGeneratedData(data);
		
		//perform Task
		data=null;
		StringBuilder builder = task.performTask(data);
		System.out.println(builder.toString());
		stop(task);
	}
	
}
