package com.phone.sessionanalyze.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.phone.sessionanalyze.constant.Constants;
import com.phone.sessionanalyze.dao.IPageSplitConvertRateDAO;
import com.phone.sessionanalyze.dao.ITaskDAO;
import com.phone.sessionanalyze.dao.factory.DAOFactory;
import com.phone.sessionanalyze.domain.PageSplitConvertRate;
import com.phone.sessionanalyze.domain.Task;
import com.phone.sessionanalyze.util.DateUtils;
import com.phone.sessionanalyze.util.NumberUtils;
import com.phone.sessionanalyze.util.ParamUtils;
import com.phone.sessionanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率
 * 1、首先获取按使用者传的taskParam过滤出来的数据
 * 2、生成页面切片匹配页面流
 * 3、根据页面切片生成单跳转化率
 * 4、数据的的存储
 */
public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        // 本地测试中获取数据
        SparkUtils.mockData(sc, sqlContext);

        // 查询任务，获取任务信息
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);

        if (task == null) {
            System.out.println(new Date() + "无法获取到taskId对应的任务信息");
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 查询指定日期范围内的用户访问的行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);

        // 对用户访问行为数据做映射，将数据映射为<sessionId, 访问行为数据>格式的session粒度的数据
        // 因为用户访问页面切片的生成，是基于每个session的访问数据生成的
        // 如果脱了了session，生成的页面切片是没有意义的
        JavaPairRDD<String, Row> sessionId2ActionRDD = getSessionId2ActionRDD(actionRDD);

        // 缓存
        sessionId2ActionRDD = sessionId2ActionRDD.cache();

        // 因为需要拿到每个session对应的行为数据，才能生成切片
        // 所以需要对session粒度的基础数据分组
        JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD =
                sessionId2ActionRDD.groupByKey();

        // 这个需求中最关键的一步，就是每个session的单跳页面切片的生成和页面流的匹配算法
        // 返回格式为：<split, 1>
        JavaPairRDD<String, Integer> pageSplitRDD =
                generateAndMatchPageSplit(sc, groupedSessionId2ActionRDD, taskParam);

        // 获取切片的访问量
        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

        // 获取起始页面的访问量
        long startPagePv = getStartPagePv(taskParam, groupedSessionId2ActionRDD);

        // 计算目标页面的各个页面切片的转化率
        // Map<String, Double>: key=各个页面切片， value=页面切片对应的转化率
        Map<String, Double> convertRateMap =
                computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);


        // 结果持久化
        persistConvertRate(taskId, convertRateMap);


        sc.stop();


    }
    /**
     * 把页面切片对应的转化率存入数据库
     * @param taskId
     * @param convertRateMap
     */
    private static void persistConvertRate(
            long taskId,
            Map<String, Double> convertRateMap) {
        // 声明一个buffer，用于存储页面流对应的切片和转化率
        StringBuffer buffer = new StringBuffer();

        for (Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
            // 获取切片
            String pageSplit = convertRateEntry.getKey();
            // 获取转化率
            double convertRate = convertRateEntry.getValue();

            // 拼接
            buffer.append(pageSplit + "=" + convertRate + "|");
        }

        // 获取拼接后的切片和转化率
        String convertRate = buffer.toString();

        // 截取掉最后的“|”
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskId);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

    /**
     * 计算页面切片转化率
     *
     * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    private static Map<String, Double> computePageSplitConvertRate(
            JSONObject taskParam,
            Map<String, Object> pageSplitPvMap,
            long startPagePv) {

        // 用于存储页面切片对应的转化率， key=各个页面切片， value=页面切片对应的转化率
        Map<String, Double> convertRateMap = new HashMap<String, Double>();

        // 获取页面流
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        // 初始化上个页面切片的访问量
        long lastPageSplitPv = 0L;

        /**
         * 求转化率：
         * 如果页面流为：1,3,5,6
         * 第一个页面切片：1_3
         * 第一个页面的转化率：3的pv / 1的pv
         */
        // 通过for循环，获取目标页面流中的各个页面切片和访问量
        for (int i = 1; i < targetPages.length; i++) {
            // 获取页面切片
            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];

            // 获取每个页面切片对应的访问量
            long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));

            // 初始化转化率
            double convertRate = 0.0;

            // 生成转化率
            if (i == 1) {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) lastPageSplitPv, 2);
            }

            convertRateMap.put(targetPageSplit, convertRate);

            lastPageSplitPv = targetPageSplitPv;
        }

        return convertRateMap;

    }

    /**
     * 获取页面流中起始页面pv
     *
     * @param taskParam
     * @param groupedSessionId2ActionRDD
     * @return
     */
    private static long getStartPagePv(
            JSONObject taskParam,
            JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD) {

        // 拿到使用者提供的页面流
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);

        // 从页面流中获取起始页面id
        final Long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD = groupedSessionId2ActionRDD.flatMap(
                new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
                    @Override
                    public Iterable<Long> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                        // 用于存储每个session访问的起始页面id
                        List<Long> list = new ArrayList<Long>();

                        // 获取对应的行为数据
                        Iterator<Row> it = tup._2.iterator();
                        while (it.hasNext()) {
                            Row row = it.next();
                            long pageId = row.getLong(3);
                            if (pageId == startPageId) {
                                list.add(pageId);
                            }
                        }

                        return list;
                    }
                });


        return startPageRDD.count();
    }

    /**
     * 页面切片的生成和页面流匹配算法的实现
     *
     * @param sc
     * @param groupedSessionId2ActionRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(
            JavaSparkContext sc,
            JavaPairRDD<String, Iterable<Row>> groupedSessionId2ActionRDD,
            JSONObject taskParam) {

        // 首先获取使用者指定的页面流
        final String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);

        // 把目标页面流广播到相应的Executor
        final Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);

        // 实现页面流匹配算法
        return groupedSessionId2ActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tup) throws Exception {
                        // 用于存储切片， 格式为：<split, 1>
                        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();

                        // 获取当前session对应的行为数据
                        Iterator<Row> it = tup._2.iterator();

                        // 获取使用者指定的页面流
                        String[] targetPages = targetPageFlowBroadcast.value().split(",");

                        /**
                         * 代码运行到这里，session的访问数据已经拿到了，
                         * 但默认情况下并没有排序,
                         * 在实现转化率的时候需要把数据按照时间顺序进行排序
                         */

                        // 把访问行为数据放到list里，便于排序
                        List<Row> rows = new ArrayList<Row>();
                        while (it.hasNext()) {
                            rows.add(it.next());
                        }

                        // 开始按照时间排序，可以用自定义的排序的方式，也可以可以用匿名内部类的方式来自定义排序，这里用的是后者
                        Collections.sort(rows, new Comparator<Row>() {
                            @Override
                            public int compare(Row o1, Row o2) {
                                String actionTime1 = o1.getString(4);
                                String actionTime2 = o2.getString(4);

                                Date date1 = DateUtils.parseTime(actionTime1);
                                Date date2 = DateUtils.parseTime(actionTime2);

                                return (int) (date1.getTime() - date2.getTime());
                            }
                        });


                        /**
                         * 生成页面切片，并和页面流进行匹配
                         */
                        // 定义一个上一个页面的id
                        Long lastPageId = null;

                        // 注意：现在拿到的rows里的数据是其中一个sessionId对应的所有行为数据
                        for (Row row : rows) {
                            long pageId = row.getLong(3);
                            if (lastPageId == null) {
                                lastPageId = pageId;
                                continue;
                            }
                            /**
                             * 生成一个页面切片
                             * 比如该用户请求的页面是：1,3,4,7
                             * 上次访问的页面id：lastPageId=1
                             * 这次请求的页面是：3
                             * 那么生成的页面切片为：1_3
                             */
                            String pageSplit = lastPageId + "_" + pageId;

                            // 对这个页面切片判断一下，是否在使用者指定的页面流中
                            for (int i = 1; i < targetPages.length; i++) {
                                // 比如说：使用者指定的页面流是：1,2，5，6
                                // 遍历的时候，从索引1开始，也就是从第二个页面开始
                                // 这样第一个页面切片就是1_2
                                String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
                                if (pageSplit.equals(targetPageSplit)) {
                                    list.add(new Tuple2<String, Integer>(pageSplit, 1));
                                    break;
                                }
                            }

                            lastPageId = pageId;
                        }

                        return list;
                    }
                });

    }

    /**
     * 生成session粒度的数据
     *
     * @param actionRDD 行为数据
     * @return <sessionId, Row>
     */

    private static JavaPairRDD<String, Row> getSessionId2ActionRDD(JavaRDD<Row> actionRDD) {


        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        });
    }

}
