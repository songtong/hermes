package com.ctrip.hermes.collector.utils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

/**
 * @author tenglinxiao
 *
 */
public class Utils {
	public static final String BU_MAPPING_FILE = "/cb_mapping.properties";
	private static Properties buMapping = new Properties(); 
	
	static {
		try {
			buMapping.load(new InputStreamReader(Utils.class.getResourceAsStream(BU_MAPPING_FILE), "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// Get bu name for a topic.
	public static String getBu(String topicName) {
		if (StringUtils.isBlank(topicName)) {
			return "others";
		}
		return topicName.split("[.]", 2)[0];
	}
	
	public static String correctBuName(String buName) {
		String mappedBu = buMapping.getProperty(buName);
		return mappedBu == null? "其他": mappedBu;
	}
	
	// Parse email address from owner data.
	public static String parseForEmailAddress(String owner) {
	    if (StringUtils.isEmpty(owner)) {
	        return null;
	    }
	    
	    String[] splits = owner.split("[/<>]");
	    
	    int position = -1;
	    for (String split : splits) {
	        if ((position = split.indexOf("@")) >= 0) {
	            // Fix legacy bug codes.
	            if ((position = split.indexOf("@", position + 1)) > -1) {
	                split = split.substring(0, position);
	            }
	            return split.trim();
	        }
	    }
	   	    
	    return null;
	}
	
	// Get recipients list for multiple owners.
	public static List<String> getRecipientsList(String... owners) {
	    List<String> emails = new ArrayList<String>();
	    for (String owner : owners) {
	        String email = parseForEmailAddress(owner);
	        if (email != null) {
	            emails.add(email);
	        }
	    }
	    return emails;
	}
	
	public static List<String> getRecipientsPhones(String... phones) {
		List<String> phoneNumbers = new ArrayList<>();
		for (String phone : phones) {
			if (phone != null) {
				phoneNumbers.add(phone);
			}
		}
		return phoneNumbers;
	}
}
