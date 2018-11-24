package com.phone.sessionanalyze.dao.impl;


import com.phone.sessionanalyze.dao.ISessionRandomExtractDAO;
import com.phone.sessionanalyze.domain.SessionRandomExtract;
import com.phone.sessionanalyze.jdbc.JDBCHelper;

/**
 * 随机抽取session的DAO实现
 * @author Administrator
 *
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {

	/**
	 * 插入session随机抽取
	 * @param sessionRandomExtract
	 */
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		
		Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
				sessionRandomExtract.getSessionid(),
				sessionRandomExtract.getStartTime(),
				sessionRandomExtract.getSearchKeywords(),
				sessionRandomExtract.getClickCategoryIds()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}
	
}
