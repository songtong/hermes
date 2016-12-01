package com.ctrip.hermes.collector.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.unidal.tuple.Pair;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.record.Record;
import com.ctrip.hermes.collector.record.RecordType;
import com.ctrip.hermes.collector.state.State;
import com.ctrip.hermes.collector.state.impl.CommandDropState;
import com.ctrip.hermes.collector.state.impl.ProduceLatencyState;
import com.ctrip.hermes.collector.utils.IndexUtils;
import com.ctrip.hermes.core.constants.CatConstants;

@Component
public class CatHttpCollectorService {
    @Autowired
    private CollectorConfiguration m_conf;
    
    public List<State> getBrokerCommandDropStatesFromResponse(Record<?> record, int fromMinute, int toMinute) throws XPathExpressionException {
        if (record == null || record.getData() == null) {
            return null;
        }
        
        XPath xPath = XPathFactory.newInstance().newXPath();
        String allEventsExpression = "/event/report[@domain='All']/machine[@ip='All']/type[@id='"
              + CatConstants.TYPE_CMD_DROP + "']/name";

        NodeList eventNodes = (NodeList) xPath.compile(allEventsExpression).evaluate(record.getData(),
              XPathConstants.NODESET);

        List<State> states = new ArrayList<>();
        
        for (int i = 0; i < eventNodes.getLength(); i++) {
            Node eventNode = eventNodes.item(i);
            String countStr = eventNode.getAttributes().getNamedItem("totalCount").getNodeValue();
            if (countStr.equals("0")) {
                continue;
            }
            
            String commandType = eventNode.getAttributes().getNamedItem("id").getNodeValue();
            String allRangesExpression = "range";

            NodeList rangeNodes = (NodeList) xPath.compile(allRangesExpression).evaluate(eventNode,
                  XPathConstants.NODESET);

            for (int j = fromMinute; j < toMinute; j++) {
                Node rangeNode = rangeNodes.item(j);
                if (rangeNode == null) {
                	continue;
                }
                
                countStr = rangeNode.getAttributes().getNamedItem("count").getNodeValue();
                // Ignore minute there is no latency.
                if (countStr.equals("0")) {
                    continue;
                }
                
                String minuteStr = rangeNode.getAttributes().getNamedItem("value").getNodeValue();
                CommandDropState state = new CommandDropState(commandType, Short.parseShort(minuteStr), Long.parseLong(countStr));
                state.setIndex(IndexUtils.getDataStoredIndex(RecordType.COMANND_DROP.getCategory().getName(), false));
                state.setTimestamp(record.getTimestamp());
                states.add(state);
            }
        }
        
        return states;
    }
    
    public List<State> getProduceLatencyStatesFromRespone(Record<?> record) throws XPathExpressionException {
        if (record == null || record.getData() == null) {
            return null;
        }
        
        XPath xPath = XPathFactory.newInstance().newXPath();
        
        String allTransactionsExpression = "/transaction/report[@domain='All']/machine[@ip='All']/type[@id='"
              + CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE_LARGE + "']/name";

        NodeList transactionNodes = (NodeList) xPath.compile(allTransactionsExpression).evaluate(record.getData(),
              XPathConstants.NODESET);

        List<State> states = new ArrayList<>();
        
        for (int i = 0; i < transactionNodes.getLength(); i++) {
            Node transactionNode = transactionNodes.item(i);
            String topic = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
            String countStr = transactionNode.getAttributes().getNamedItem("totalCount").getNodeValue();
            if (countStr.equals("0") || topic.equalsIgnoreCase("All")) {
                continue;
            }
            
            String avgStr = transactionNode.getAttributes().getNamedItem("avg").getNodeValue();
            String minStr = transactionNode.getAttributes().getNamedItem("min").getNodeValue();
            String maxStr = transactionNode.getAttributes().getNamedItem("max").getNodeValue();

            ProduceLatencyState state = new ProduceLatencyState(topic, Long.parseLong(countStr), Double.parseDouble(avgStr), Double.parseDouble(minStr), Double.parseDouble(maxStr));
            state.setIndex(IndexUtils.getDataStoredIndex(RecordType.PRODUCE_LATENCY.getCategory().getName(), false));
            state.setTimestamp(record.getTimestamp());
            states.add(state);
        }
        
        return states;
    }
    
    public Map<String, Long> getProduceElapseStatesFromResponse(Record<?> record) throws XPathExpressionException {
    	 if (record == null || record.getData() == null) {
             return null;
         }
    	 
         Map<String, Long> elapses = new HashMap<>();
         XPath xPath = XPathFactory.newInstance().newXPath();
         
         String allTransactionsExpression = "/transaction/report[@domain='All']/machine[@ip='All']/type[@id='"
               + CatConstants.TYPE_MESSAGE_PRODUCE_ELAPSE + "']/name";

         NodeList transactionNodes = (NodeList) xPath.compile(allTransactionsExpression).evaluate(record.getData(),
               XPathConstants.NODESET);

         for (int i = 0; i < transactionNodes.getLength(); i++) {
             Node transactionNode = transactionNodes.item(i);
             String topic = transactionNode.getAttributes().getNamedItem("id").getNodeValue();
             String countStr = transactionNode.getAttributes().getNamedItem("totalCount").getNodeValue();

             if (countStr.equals("0") || topic.equalsIgnoreCase("All")) {
                 continue;
             }
             
             elapses.put(topic, Long.parseLong(countStr));
         }
         
         return elapses;
    }
}
