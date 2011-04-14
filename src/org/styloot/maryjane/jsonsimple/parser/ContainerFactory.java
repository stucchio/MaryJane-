package org.styloot.maryjane.jsonsimple.parser;

import java.util.List;
import java.util.Map;

/**
 * Container factory for creating containers for JSON object and JSON array.
 * 
 * @see org.styloot.maryjane.jsonsimple.parser.JSONParser#parse(java.io.Reader, ContainerFactory)
 * 
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public interface ContainerFactory {
	/**
	 * @return A Map instance to store JSON object, or null if you want to use org.styloot.maryjane.jsonsimple.JSONObject.
	 */
	Map createObjectContainer();
	
	/**
	 * @return A List instance to store JSON array, or null if you want to use org.styloot.maryjane.jsonsimple.JSONArray. 
	 */
	List creatArrayContainer();
}
