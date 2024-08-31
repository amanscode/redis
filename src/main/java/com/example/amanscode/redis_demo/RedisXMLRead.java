package com.example.amanscode.redis_demo;

import java.io.File;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;

public class RedisXMLRead {
	public static void main(String[] args) {
		RedisClusterClient redisClient = RedisClusterClient.create("redis://10.32.141.15:7770,10.32.141.15:7771,10.32.141.15:7772");
//		RedisClusterClient redisClient = RedisClusterClient.create("redis://10.32.141.15:7770");

//		ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
//				.enablePeriodicRefresh(Duration.ofSeconds(15)).enableAllAdaptiveRefreshTriggers()
//				.closeStaleConnections(true).build();
//		redisClient.setOptions(ClusterClientOptions.builder().topologyRefreshOptions(topologyRefreshOptions).build());

		StatefulRedisClusterConnection<String, Object> connection = redisClient.connect(new SerializedObjectCodec());
		RedisAdvancedClusterCommands<String, Object> syncCommands = connection.sync();
		RedisAdvancedClusterAsyncCommands<String, Object> asyncCommands = connection.async();
		
//		String streamName = "syncclientdr";
		String streamName = "syncclientprimary";
		
		try {
//			Document doc = buildDoc();
//			System.out.println("Adding start ****************************************");
//			addXAdd(syncCommands, streamName, doc, "PUT");
//			System.out.println("Adding done ****************************************");
//			System.out.println();
			System.out.println("Reading start ****************************************");
			readXRange(syncCommands, streamName, "-", "+");
			System.out.println("Reading done ****************************************");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Document buildDoc() throws Exception {
		File file = new File("C:\\Eclipse Workspaces\\Workspace_2\\LettuceDemo\\files\\xmlfile.xml");
		DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
        Document document = documentBuilder.parse(file);
        Element e = document.createElement("CreationTime");
        e.setTextContent(new Date().toString());
        document.appendChild(e);
        System.out.println("document :");
        prettyPrint(document);
        return document;
	}
	
	public static final void prettyPrint(Document xml) throws Exception {
        Transformer tf = TransformerFactory.newInstance().newTransformer();
        tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        tf.setOutputProperty(OutputKeys.INDENT, "yes");
        Writer out = new StringWriter();
        tf.transform(new DOMSource(xml), new StreamResult(out));
        System.out.println(out.toString());
    }
	
	public static String addXAdd(RedisAdvancedClusterCommands<String, Object> syncCommands, String streamName, Document doc, String requestType) {
		XAddArgs xaddArg = new XAddArgs();
		xaddArg.maxlen(100000000L);
		Map<String, Object> data = new HashMap<String, Object>();
		data.put("requestType", requestType);
		data.put("xmlData", doc);
		String response = syncCommands.xadd(streamName, xaddArg, data);
		System.out.println("Response of XADD with maxlen : " + response);
		return response;
	}
	
	public static List<StreamMessage<String, Object>> readXRange(RedisAdvancedClusterCommands<String, Object> syncCommands, String streamName, String start, String end) throws Exception {
		System.out.println(); System.out.println("Reading everything using XRANGE between " + start + " & " + end + " **********************");
		Range<String> range = Range.create(start, end);
		Limit limit = Limit.from(20L);
		List<StreamMessage<String, Object>> streamMessagesList = syncCommands.xrange(streamName, range, limit);
		int listSize = streamMessagesList.size();
		for (int i = 0; i < listSize; i++) {
            StreamMessage<String, Object> streamMessage = streamMessagesList.get(i);
            String _streamName = streamMessage.getStream();
            String _streamID = streamMessage.getId();
            System.out.println(_streamName + " : " + _streamID);
            Map<String, Object> _data = streamMessage.getBody();
            for(Map.Entry<String, Object> entry : _data.entrySet()) {
            	System.out.println("Field: " + entry.getKey() + ", Value: " + entry.getValue());
            	String s = (String) entry.getValue();
            	System.out.println("s : " + s);
//            	System.out.println("s.something : " + s.length());
            	if("xmlData".equalsIgnoreCase(entry.getKey())) {
//            		Document d = (Document)entry.getValue();
//            		System.out.println("d : " + d);
//                	prettyPrint(d);
            	}
            }
        }
		return streamMessagesList;
	}
	
}
