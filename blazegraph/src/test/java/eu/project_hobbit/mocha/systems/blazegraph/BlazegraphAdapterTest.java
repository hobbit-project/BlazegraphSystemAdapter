package eu.project_hobbit.mocha.systems.blazegraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


/**
 * Tests all taks of the adapter
 * 
 * @author f.conrads
 *
 */
public class BlazegraphAdapterTest {

	private void start(BlazegraphAdapter task) throws Exception {
		task.internalInit();
		task.startServer(); 
		
	}

	private void stop(AbstractBlazegraphAdapterTask task) throws Exception {
		task.closeServer();
	}

	
	private List<String> convertResults(byte[] data, String[] vars){
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			baos.write(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return convertResults(baos, vars);
	}
	
	
	private List<String> convertResults(ByteArrayOutputStream baos, String[] vars){
		List<String> results = new LinkedList<String>();
		String jsonStr = baos.toString();
		JsonElement json =  new JsonParser().parse(jsonStr);
		JsonArray bindings = json.getAsJsonObject().get("results").getAsJsonObject().get("bindings").getAsJsonArray();
		for(JsonElement binding : bindings) {
			for(String var : vars) {
				results.add(binding.getAsJsonObject().get(var).getAsJsonObject().get("value").getAsString());
			}
		}
		
		return results;
	}
	
	/**
	 * Tests the first task of the adapter
	 * 
	 * @throws Exception
	 */
	@Test
	public void task1Test() throws Exception {
		BlazegraphAdapter task = new BlazegraphAdapter();
		start(task);
		try {
			byte[] data = "PREFIX dc: <http://purl.org/dc/elements/1.1/> INSERT DATA { <http://example/egbook3> dc:title  \"This is an example title\" }"
					.getBytes();
			task.insertData(data);

			data = "SELECT ?s {?s <http://purl.org/dc/elements/1.1/title> ?o}".getBytes();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			task.select(data, baos);
			List<String> results = convertResults(baos, new String[] {"s"});
			assertTrue(results.contains("http://example/egbook3"));
			assertEquals(1, results.size());
		} finally {
			stop(task);
		}
	}


	/**
	 * Tests the second task of the adapter
	 * 
	 * @throws Exception
	 */
	@Test
	public void task2Test() throws Exception {
		BlazegraphAdapter task = new BlazegraphAdapter();
		start(task);
		try {
			byte[] data = "<http://example/egbook3> <http://purl.org/dc/elements/1.1/title>  \"This is an example titleA\".\n"
					.getBytes();
			// bulkload
			task.saveData(data);
			// sparql
			String query  = "SELECT ?s {?s <http://purl.org/dc/elements/1.1/title> ?o}";
			data = task.sparql("", query).toByteArray();
			List<String> results = convertResults(data, new String[] {"s"});
			assertTrue(results.contains("http://example/egbook3"));
			assertEquals(1, results.size());
			
			// update
			query = "PREFIX dc: <http://purl.org/dc/elements/1.1/> INSERT DATA { <http://example/egbook4> dc:title  \"ab\" }";
			task.insert(query);
			
			//check
			query = "SELECT (COUNT(?s) AS ?co) {?s <http://purl.org/dc/elements/1.1/title> ?o}";
			data = task.sparql("", query).toByteArray();
			results = convertResults(data, new String[] {"co"});
			assertTrue(results.contains("2"));
		} finally {
			stop(task);
		}
	}

	/**
	 * Tests the third task of the adapter
	 * 
	 * @throws Exception
	 */
	@Test
	public void task3Test() throws Exception {
		BlazegraphAdapter task = new BlazegraphAdapter();
		task.internalInit();
		try {
			// data: length:name:length:content
			byte[] name = "http://urn.1".getBytes();
			byte[] nameLen = ByteBuffer.allocate(4).putInt(name.length).array();
			byte[] content = "<http://example/egbook4> <http://purl.org/dc/elements/1.1/title>  \"a\".\n".getBytes();
			byte[] contentLen = ByteBuffer.allocate(4).putInt(content.length).array();
			byte[] data = ArrayUtils.addAll(nameLen, name);
			data = ArrayUtils.addAll(data, contentLen);
			data = ArrayUtils.addAll(data, content);
			task.saveData(data);
			task.loadDataset("http://test.com.1");

			
			name = "http://urn.2".getBytes();
			nameLen = ByteBuffer.allocate(4).putInt(name.length).array();
			content = "<http://example/egbook5> <http://purl.org/dc/elements/1.1/title>  \"a\" . \n".getBytes();
			contentLen = ByteBuffer.allocate(4).putInt(content.length).array();
			data = ArrayUtils.addAll(nameLen, name);
			data = ArrayUtils.addAll(data, contentLen);
			data = ArrayUtils.addAll(data, content);
			task.saveData(data);
			task.loadDataset("http://test.com.2");
			start(task);
			data = "SELECT ?s FROM <http://test.com.2> {?s <http://purl.org/dc/elements/1.1/title> ?o}".getBytes();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			task.select(data, baos);
			List<String> results = convertResults(baos, new String[] {"s"});
			assertTrue(results.contains("http://example/egbook5"));
			assertEquals(1, results.size());
		} finally {
			stop(task);
		}
	}

	/**
	 * Tests the fourth task of the adapter
	 * 
	 * @throws Exception
	 */
//	@Test
//	public void task4Test() throws Exception {
//		BlazegraphAdapter task = new BlazegraphAdapter();
//		start(task);
//		try {
//			// bulk load
//			byte[] data = "<http://example/egbook3> <http://purl.org/dc/elements/1.1/title>  \"This is an example title\". \n <http://example/egbook1> <http://purl.org/dc/elements/1.1/title>  \"This is an example title3\"."
//					.getBytes();
//			task.receiveGeneratedData(data);
//
//			// perform Task URI
//			data = "SELECT ?s {?s <http://purl.org/dc/elements/1.1/title> ?o}".getBytes();
//			StringBuilder builder = task.performTask(data);
//			System.out.println(builder.toString());
//			assertEquals("http://example/egbook3, http://example/egbook1", builder.toString());
//			// perform Task COUNT
//			data = "SELECT (COUNT(?s) AS ?co) {?s <http://purl.org/dc/elements/1.1/title> ?o}".getBytes();
//			builder = task.performTask(data);
//			System.out.println(builder.toString());
//			assertEquals("2", builder.toString());
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			stop(task);
//		}
//	}

}
