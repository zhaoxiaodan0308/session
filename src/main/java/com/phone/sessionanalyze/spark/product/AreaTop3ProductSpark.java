package com.phone.sessionanalyze.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.phone.sessionanalyze.conf.ConfigurationManager;
import com.phone.sessionanalyze.constant.Constants;
import com.phone.sessionanalyze.dao.IAreaTop3ProductDAO;
import com.phone.sessionanalyze.dao.ITaskDAO;
import com.phone.sessionanalyze.dao.factory.DAOFactory;
import com.phone.sessionanalyze.domain.AreaTop3Product;
import com.phone.sessionanalyze.domain.Task;
import com.phone.sessionanalyze.util.ParamUtils;
import com.phone.sessionanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 区域热门top3商品
 * 1、获取按照使用者指定日期范围内的点击行为数据
 * 2、获取城市信息（区域）
 * 3、按照区域来分组并排序
 * 4、得到区域top3热门商品
 * 5、持久化
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSQLContext(sc.sc());

        //local模式下获取数据
        SparkUtils.mockData(sc, sqlContext);

        //创建自定义函数
        sqlContext.udf().register("group_concat_distinct",new GroupConcatDistinctUDAF());
        sqlContext.udf().register("ConcatLongStringUDF",new ConcatLongStringUDF(),DataTypes.StringType);
        sqlContext.udf().register("get_json_object",new GetJsonObjectUDF(),DataTypes.StringType);

        //获取task信息
        long taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskId);

        //将task信息封装为JSONObject对象
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //获取使用者指定的开始时间和结束时间
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        //查询用户指定日期范围内的点击行为数据：<city_id,<city_id,click_product_id>>
        JavaPairRDD<Long, Row> cityId2ClickActionRDD =
                getCityId2ClickActionRDD(sqlContext, startDate, endDate);

        //从数据库获取城市信息，返回格式为:<city_id,cityInfo>
        JavaPairRDD<Long, Row> cityId2CityInfoRDD = getCityId2CityInfoRDD(sqlContext);

        // 生成点击商品基础信息临时表tmp_click_product_basic: city_id \ city_name \ area \ product_id
        generateTempClickProductBasicTable(sqlContext, cityId2ClickActionRDD, cityId2CityInfoRDD);


        // 生成各区域商品点击次数:tmp_area_product_click_count:area, product_id, click_count, city_info
        generateTempAreaProductClickCountTable(sqlContext);

        // 生成包含完整商品信息的各区域各商品点击次数临时表：tmp_area_fullprod_click_count
        // area,product_id,click_count,city_infos,product_name,product_status
        generateTempAreaFullProductClickCountTable(sqlContext);

        // 使用开窗函数获取各个区域点击次数top3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);

        // 数据的持久化可以分布式的存储到数据库中，
        // 也可以在driver端进行存储
        // 该需求的结果数据非常少，可以选择后者进行操作
        List<Row> rows = areaTop3ProductRDD.collect();

        // 存储
        persistAreaTop3Product(taskId, rows);


        sc.stop();


    }

    /**
     * 把结构存储到数据库
     * @param taskId
     * @param rows
     */

    private static void persistAreaTop3Product(long taskId, List<Row> rows) {

        List<AreaTop3Product> list = new ArrayList<AreaTop3Product>();

        for (Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskid(taskId);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductid(row.getLong(2));
            areaTop3Product.setClickCount(row.getLong(3));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));

            list.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areaTop3ProductDAO.insertBatch(list);
    }

    /**
     * 使用开窗函数获取各个区域点击次数top3的热门商品
     * @param sqlContext
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {
        /**
         * 使用开窗函数进行子查询
         * 按照area进行分组，给每个分组内的数据按照点击次数进行降序排序，打一个行标
         * 然后在外层查询中，过滤出各个组内行标排名前3的数据
         */
        /**
         * 按照区域进行区域分级：交易量划分
         * 华北、华东、华南、华中、西北、西南、东北
         * A级：华北、华东
         * B级：华南、华中
         * C级：西北、西南
         * D级：东北
         */
        String sql =
                "select " +
                        "area, " +
                        "case " +
                        "when area='华北' or area='华东' then 'A level' " +
                        "when area='华南' or area='华中' then 'B level' " +
                        "when area='西北' or area='西南' then 'C level' " +
                        "else 'D level' " +
                        "end area_level, " +
                        "product_id, " +
                        "click_count, " +
                        "city_infos, " +
                        "product_name, " +
                        "product_status " +
                        "from (" +
                        "select " +
                        "area, " +
                        "product_id, " +
                        "click_count, " +
                        "city_infos, " +
                        "product_name, " +
                        "product_status, " +
                        "row_number() over (partition by area order by click_count desc) rank " +
                        "from tmp_area_fullprod_click_count " +
                        ") t " +
                        "where rank <= 3";

        DataFrame df = sqlContext.sql(sql);

        return df.javaRDD();
    }

    /**
     * 生成包含完整商品信息的各区域各商品点击次数临时表
     *
     * @param sqlContext
     */
    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        /**
         * 将之前得到的各区域各商品点击次数的product_id字段去关联商品信息表的product_id, product_name, product_status
         * 其中product_status要特殊处理，0，1分别代表了自营和第三方商品，放在一个json里
         * GetJsonObjectUDF(),从json里获取指定字段的值
         * if()函数判断，如果product_status是0，就是自营商品，如果是1，就是第三方商品
         * 该表的字段有：
         * area,product_id,click_count,city_infos,product_name,product_status
         */
        String sql =
                "select " +
                        "tapcc.area, " +
                        "tapcc.product_id, " +
                        "tapcc.click_count, " +
                        "tapcc.city_infos, " +
                        "pi.product_name, " +
                        "if(get_json_object(pi.extend_info,'product_status')='0'," +
                        "'self','third party') product_status " +
                        "from tmp_area_product_click_count tapcc " +
                        "join product_info pi " +
                        "on tapcc.product_id=pi.product_id";

        DataFrame df = sqlContext.sql(sql);

        df.registerTempTable("tmp_area_fullprod_click_count");

    }


    /**
     * 生成各区域商品点击次数
     *
     * @param sqlContext
     */
    private static void generateTempAreaProductClickCountTable(SQLContext sqlContext) {
        /**
         * 按照area和product_id这两个字段进行分组
         * 计算出各区域商品的点击次数
         * 可以获取到每个区域下的每个商品的城市信息，并拼接为字符串
         */
        String sql =
                "select " +
                        "area, " +
                        "product_id, " +
                        "count(*) click_count, " +
                        "group_concat_distinct(ConcatLongStringUDF(city_id,city_name,':')) city_infos " +
                        "from tmp_click_product_basic " +
                        "group by area, product_id";

        DataFrame df = sqlContext.sql(sql);

        // area, product_id, click_count, city_info
        df.registerTempTable("tmp_area_product_click_count");

    }

    /**
     * 生成点击商品基础信息临时表
     *
     * @param sqlContext
     * @param cityId2ClickActionRDD <city_id,<city_id,click_product_id>>
     * @param cityId2CityInfoRDD    <city_id,cityInfo>
     */
    private static void generateTempClickProductBasicTable(
            SQLContext sqlContext,
            JavaPairRDD<Long, Row> cityId2ClickActionRDD,
            JavaPairRDD<Long, Row> cityId2CityInfoRDD) {

        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD =
                cityId2ClickActionRDD.join(cityId2CityInfoRDD);

        // 将上面join后的RDD转换为一个JavaRDD<Row>，这样才能映射为临时表
        JavaRDD<Row> mappedRDD = joinedRDD.map(new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
            @Override
            public Row call(Tuple2<Long, Tuple2<Row, Row>> tup) throws Exception {
                long cityId = tup._1;
                Row clickAction = tup._2._1;
                Row cityInfo = tup._2._2;

                long productId = clickAction.getLong(1);
                String cityName = cityInfo.getString(1);
                String area = cityInfo.getString(2);

                return RowFactory.create(cityId, cityName, area, productId);
            }
        });

        // 生成schema信息
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(structFields);

        DataFrame dataFrame = sqlContext.createDataFrame(mappedRDD, schema);

        dataFrame.registerTempTable("tmp_click_product_basic");

    }

    /**
     * 获取城市信息
     *
     * @param sqlContext
     * @return
     */
    private static JavaPairRDD<Long, Row> getCityId2CityInfoRDD(SQLContext sqlContext) {

        // 构建jdbc配置信息
        String url = null;
        String user = null;
        String password = null;

        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        // 用于存储请求mysql的连接配置信息
        Map<String, String> options = new HashMap<String, String>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", password);

        // 获取city_info信息
        DataFrame cityInfoDF = sqlContext.read().format("jdbc").options(options).load();

        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();
        JavaPairRDD<Long, Row> cityId2CityInfoRDD = cityInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(Long.valueOf(String.valueOf(row.get(0))), row);
                    }
                });


        return cityId2CityInfoRDD;
    }

    /**
     * 查询用户指定日期范围内的点击行为数据
     *
     * @param sqlContext
     * @param startDate
     * @param endDate
     * @return
     */
    private static JavaPairRDD<Long, Row> getCityId2ClickActionRDD(
            SQLContext sqlContext,
            String startDate,
            String endDate) {
        // 从user_visit_action表中查询用户访问行为数据
        // 第一个限定：click_product_id限定为不为空的访问行为，这个字段的值就代表点击商品行为
        // 第二个限定：在使用者指定日期范围内的数据

        String sql = "select " +
                "city_id, " +
                "click_product_id , product_id " +
                "from user_visit_action " +
                "where click_product_id is not null " +
                "and session_date>='" + startDate + "' " +
                "and session_date<='" + endDate + "'";

        DataFrame clickActionDF = sqlContext.sql(sql);

        // 把生成的DataFrame转换为RDD
        JavaRDD<Row> clickActionRDD = clickActionDF.javaRDD();

        JavaPairRDD<Long, Row> cityId2ClickActionRDD = clickActionRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        return new Tuple2<Long, Row>(row.getLong(0), row);
                    }
                });

        return cityId2ClickActionRDD;

    }
}
