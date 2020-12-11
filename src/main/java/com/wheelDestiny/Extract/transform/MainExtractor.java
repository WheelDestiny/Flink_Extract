package com.wheelDestiny.Extract.transform;

import com.wheelDestiny.Extract.sink.*;
import com.wheelDestiny.Extract.source.XpathSourceFunction;
import com.wheelDestiny.Extract.util.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MainExtractor {

    //reids的侧输出流
    private static final OutputTag<RedisRecord> RedisOutput = new OutputTag<RedisRecord>("RedisOutput") {
    };
    //HBase的侧输出流
    private static final OutputTag<HBaseRecord> HBaseOutput = new OutputTag<HBaseRecord>("HBaseOutput") {
    };
    //ES的侧输出流
    private static final OutputTag<ESRecord> ESOutput = new OutputTag<ESRecord>("ESOutput") {
    };
    //Mysql的侧输出流
    private static final OutputTag<MysqlRecord> MysqlOutput = new OutputTag<MysqlRecord>("MysqlOutput") {
    };


    public static void main(String[] args) throws Exception {
        MapStateDescriptor<String, HashMap<String, HashSet<String>>> xpathBroadCastInfo = new MapStateDescriptor<>("xpathBroadCast",
                BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<HashMap<String, HashSet<String>>>() {
        }));


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //保存EXACTLY_ONCE
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次ck之间的间隔，不会重叠
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);
        //每次ck的超时时间
        checkpointConfig.setCheckpointTimeout(20000L);
        //每次ck执行失败，程序是否停止
        checkpointConfig.setFailOnCheckpointingErrors(true);
        //job在执行CANCLE的时候是否删除数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //指定保存ck的存储格式，这个是默认的
        MemoryStateBackend stateBackend = new MemoryStateBackend(10 * 1024 * 1024, false);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3,//number of restart attempts
                        org.apache.flink.api.common.time.Time.of(0, TimeUnit.SECONDS)//delay
                )
        );


        DataStreamSource<HashMap<String, HashMap<String, HashSet<String>>>> xpathSource = env.addSource(new XpathSourceFunction());

        //文件流变成广播
        BroadcastStream<HashMap<String, HashMap<String, HashSet<String>>>> xpathBroadcast = xpathSource.broadcast(xpathBroadCastInfo);


        //kafka流的配置
        Properties properties = new Properties();

        properties.setProperty("flink.partition-discovery.interval-millis", "30000");
        properties.setProperty("bootstrap.servers", "s1.hadoop:9092,s3.hadoop:9092,s4.hadoop:9092,s5.hadoop:9092,s6.hadoop:9092,s7.hadoop:9092,s8.hadoop:9092");
        properties.setProperty("group.id", "wheelDestiny_Flink");

        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<>("hainiu_html", new SimpleStringSchema(), properties);

        //设置从offset结束开始读数据
        kafkaSource.setStartFromLatest();

        //创建kafka流
        DataStreamSource<String> kafkaInput = env.addSource(kafkaSource);

        //转化为redis的数据格式，抽取xpath格式
        SingleOutputStreamOperator<MysqlRecord> redisProcess = kafkaInput.process(new ProcessFunction<String, MysqlRecord>() {
            @Override
            public void processElement(String value, Context ctx, Collector<MysqlRecord> out) throws Exception {
                //md5\001url\001html
                String[] split = value.split("\001");

                String url = split[1];

                URL u = new URL(url);
                String host = u.getHost();

                String txpath = "";
                ArrayList<String> fxpaths = new ArrayList<>();

                Map<String, String> xpathMap = HtmlContentExtractor.generateXpath(split[2]);

                if (xpathMap == null) {
                    System.out.println("抽取失败");
                } else {
                    //抽取成功
                    for (Map.Entry<String, String> m : xpathMap.entrySet()) {
                        String v = m.getValue();
                        if (v.equals("正文")) {
                            txpath = m.getKey();
                        } else {
                            fxpaths.add(m.getKey());
                        }
                    }
                    //输出到Redis的侧输出
                    ctx.output(RedisOutput, new RedisRecord(host, txpath, fxpaths));
                }
            }
        });


        //创建用于正文抽取的流
        SingleOutputStreamOperator<Tuple2<String, String>> keyBy = kafkaInput.map(new MapFunction<String, Tuple2<String, String>>() {
            //md5\001url\001html
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] split = s.split("\001");

                String url = split[1];

                URL u = new URL(url);
                String host = u.getHost();


                return Tuple2.of(host, s);
            }
        });

        //将keyBy和广播流合并xpathBroadcast
        BroadcastConnectedStream<Tuple2<String, String>, HashMap<String, HashMap<String, HashSet<String>>>> connect = keyBy.connect(xpathBroadcast);
        //抽出正文的逻辑，在这个流中将数据处理成HBase和ES的泛型
        SingleOutputStreamOperator<Record> operator = connect.process(
                new BroadcastProcessFunction<Tuple2<String, String>, HashMap<String, HashMap<String, HashSet<String>>>, Record>() {
                    @Override
                    public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Record> out) throws Exception {
                        //mysqlRecord的属性
                        Long filtered = 0L;
                        Long extract = 0L;
                        Long emptyContext = 0L;
                        Long noMatchXpath = 0L;

                        //url, host, content, domain, urlMd5, value.eventTime, html
                        String url = "";
                        String host = "";
                        String content = "";
                        String domain = "";
                        String urlMd5 = "";
                        String html = "";

                        //md5\001url\001html
                        String[] split = value.f1.split("\001");

                        String currentMD5 = DigestUtils.md5Hex(split[1] +"\001"+ split[2]);
//                        System.out.println("currentMD5"+currentMD5);
//                        System.out.println("split[0]"+split[0]);

                        if (!currentMD5.equals(split[0])) {
                            filtered = 1L;
                        } else {
                            url = split[1];
                            host = value.f0;

                            URL urlT = new URL(url);
                            urlMd5 = split[0];
                            domain = JavaUtil.getDomainName(urlT);
                            html = split[2];

                            if (!"".equals(html)) {
                                // 从广播变量中得到xpath信息
                                ReadOnlyBroadcastState<String, HashMap<String, HashSet<String>>> broadcastState = ctx.getBroadcastState(xpathBroadCastInfo);
                                if (broadcastState.contains(host)) {
                                    HashMap<String, HashSet<String>> hostXpathInfo = broadcastState.get(host);
                                    HashSet<String> truePathInfo = hostXpathInfo.getOrDefault("true", new HashSet<>());
                                    HashSet<String> falsePathInfo = hostXpathInfo.getOrDefault("false", new HashSet<>());

                                    Document doc = Jsoup.parse(html);

                                    content = Util.getContent(doc, truePathInfo, falsePathInfo);
                                    if (content.trim().length() < 10) {
                                        //如果抽取出的正文长度小于10,抽取内容大概率不正确，记录并计数
                                        System.out.println("抽取失败" + url);
                                        emptyContext = 1L;
                                    } else {
                                        //抽取成功
                                        extract = 1L;
                                        HBaseRecord hBaseRecord = new HBaseRecord(url, host, content, domain, urlMd5, System.currentTimeMillis(), html);
                                        ESRecord esRecord = new ESRecord(url, host, content, domain, urlMd5, System.currentTimeMillis());

                                        System.out.println("抽取成功：");
                                        ctx.output(HBaseOutput, hBaseRecord);
                                        ctx.output(ESOutput, esRecord);
                                    }
                                } else {
                                    noMatchXpath = 1L;
                                }
                            }
                            //输出到mysql
                            MysqlRecord mysqlRecord = new MysqlRecord(host, filtered, extract, emptyContext, noMatchXpath, 1L);
//                            System.out.println("111"+value1);
                            ctx.output(MysqlOutput, mysqlRecord);
                        }
                    }

                    @Override
                    public void processBroadcastElement(HashMap<String, HashMap<String, HashSet<String>>> value, Context ctx, Collector<Record> out) throws Exception {
                        BroadcastState<String, HashMap<String, HashSet<String>>> broadcastState = ctx.getBroadcastState(xpathBroadCastInfo);
                        for (Map.Entry<String, HashMap<String, HashSet<String>>> mapEntry : value.entrySet()) {
                            broadcastState.put(mapEntry.getKey(), mapEntry.getValue());
                        }
                    }
                }
        );

        //创建redisSource
        RedisRichSinkFunction redisRichSinkFunction = new RedisRichSinkFunction();
        //保存数据到redis
        redisProcess.getSideOutput(RedisOutput).addSink(redisRichSinkFunction);

        //保存数据到HBase
        operator.getSideOutput(HBaseOutput).addSink(new HBaseRichSinkFunction());
        //保存数据到ES
        operator.getSideOutput(ESOutput).addSink(ESSink.apply());
        //保存数据到Mysql
        SingleOutputStreamOperator<MysqlRecord> mysqlProcess = operator.getSideOutput(MysqlOutput)
                //设定窗口时间5s一触发
                .timeWindowAll(Time.seconds(5))
                .process(new ProcessAllWindowFunction<MysqlRecord, MysqlRecord, TimeWindow>() {
                    HashMap<String, Long> hostScan = null;
                    HashMap<String, Long> hostFilter = null;
                    HashMap<String, Long> hostExtract = null;
                    HashMap<String, Long> hostEmpty = null;
                    HashMap<String, Long> hostNoMatch = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hostScan = new HashMap<>();
                        hostFilter = new HashMap<>();
                        hostExtract = new HashMap<>();
                        hostEmpty = new HashMap<>();
                        hostNoMatch = new HashMap<>();
                    }

                    @Override
                    public void close() throws Exception {
                        hostScan.clear();
                        hostFilter.clear();
                        hostExtract.clear();
                        hostEmpty.clear();
                        hostNoMatch.clear();
                    }

                    @Override
                    public void process(Context context, Iterable<MysqlRecord> elements, Collector<MysqlRecord> out) throws Exception {
                        Iterator<MysqlRecord> iterator = elements.iterator();

                        for (;iterator.hasNext();){
                            MysqlRecord next = iterator.next();
                            System.out.println(next.host());

                            Long scan = hostScan.getOrDefault(next.host(), 0L);
                            Long filter = hostFilter.getOrDefault(next.host(), 0L);
                            Long extract = hostExtract.getOrDefault(next.host(), 0L);
                            Long empty = hostEmpty.getOrDefault(next.host(), 0L);
                            Long noMatch = hostNoMatch.getOrDefault(next.host(), 0L);

                            hostScan.put(next.host(), scan + next.scan());
                            hostFilter.put(next.host(), filter + next.filtered());
                            hostExtract.put(next.host(), extract + next.extract());
                            hostEmpty.put(next.host(), empty + next.emptyContext());
                            hostNoMatch.put(next.host(), noMatch + next.noMatchXpath());
                        }

                        //每条被处理过的数据都向mysql插入一次
                        for (Map.Entry<String, Long> entry : hostScan.entrySet()) {
                            Long filter = hostFilter.getOrDefault(entry.getKey(), 0L);
                            Long extract = hostExtract.getOrDefault(entry.getKey(), 0L);
                            Long empty = hostEmpty.getOrDefault(entry.getKey(), 0L);
                            Long noMatch = hostNoMatch.getOrDefault(entry.getKey(), 0L);
                            MysqlRecord t = new MysqlRecord(entry.getKey(), filter, extract, empty, noMatch, entry.getValue());
                            System.out.println("本次统计结果："+t);

                            out.collect(t);
                        }
                        hostScan.clear();
                        hostFilter.clear();
                        hostExtract.clear();
                        hostEmpty.clear();
                        hostNoMatch.clear();
                    }
                });
        //这里的数据量是很少的，我们把这些数据拉回driver来插入mysql，
        // 基于两点，
        //  第一，并发的插入mysql本身就有一些要处理的问题，并发写入触发的锁，导致的效率降低问题等
        //  第二，实际生产环境中并不是所有的机器节点都有插入mysql的权限，我们无法保证TaskManager运行在哪个具体的节点上，所以选择拉回driver
        Iterator<MysqlRecord> mysqlCollect = DataStreamUtils.collect(mysqlProcess);

        while (mysqlCollect.hasNext()) {
            MysqlRecord value = mysqlCollect.next();
            System.out.println("本次统计信息：" + value);
            DBUtil.insertIntoMysqlByJdbc(value);
        }


        env.execute();
    }
}
