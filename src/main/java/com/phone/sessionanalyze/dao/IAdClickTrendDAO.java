package com.phone.sessionanalyze.dao;

import com.phone.sessionanalyze.domain.AdClickTrend;

import java.util.List;

/**
 * 广告点击趋势DAO接口
 * @author Administrator
 *
 */
public interface IAdClickTrendDAO {

	void updateBatch(List<AdClickTrend> adClickTrends);
	
}
