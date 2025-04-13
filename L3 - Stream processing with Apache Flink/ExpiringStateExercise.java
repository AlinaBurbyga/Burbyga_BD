package com.ververica.flinktraining.exercises.datastream_java.process;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ExpiringStateExercise extends ExerciseBase {

  // Тэг для потока поездок, для которых не найдена соответствующая плата
  static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
  // Тэг для потока плат, для которых не найдена соответствующая поездка
  static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

  public static void main(String[] args) throws Exception {

    // Получение параметров командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
    final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

    final int maxEventDelay = 60;           // Максимальная задержка событий 
    final int servingSpeedFactor = 600;   // Фактор скорости обработки 

    // Настройка окружения для потоковой обработки
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Установка временной характеристики потока
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // Установка количества параллельных задач
    env.setParallelism(ExerciseBase.parallelism);

    // Создание потока данных о поездках из файла
    DataStream<TaxiRide> rides = env
        .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
        .filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
        .keyBy(ride -> ride.rideId);

    // Создание потока данных о плате за поездки из файла
    DataStream<TaxiFare> fares = env
        .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
        .keyBy(fare -> fare.rideId);

    // Соединение потоков поездок и платы и обработка 
    SingleOutputStreamOperator processed = rides
        .connect(fares)
        .process(new EnrichmentFunction());

    // Вывод в консоль потока плат, для которых не найдена соответствующая поездка
    printOrTest(processed.getSideOutput(unmatchedFares));

    env.execute("ExpiringStateSolution (java)");
  }

  // Функция соединение поездок и платы
  public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

    // Состояние для хранения поездки 
    private ValueState<TaxiRide> rideState;
    // Состояние для хранения платы за поездку 
    private ValueState<TaxiFare> fareState;

    @Override
    public void open(Configuration config) {
      rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
      fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
    }

    @Override
    public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
      // Получение платы из состояния
      TaxiFare fare = fareState.value();
      if (fare != null) {
        fareState.clear();
        context.timerService().deleteEventTimeTimer(fare.getEventTime());
        out.collect(new Tuple2(ride, fare));
      } else {
        rideState.update(ride);
        // Регистрируем таймер на время события 
        context.timerService().registerEventTimeTimer(ride.getEventTime());
      }
    }
