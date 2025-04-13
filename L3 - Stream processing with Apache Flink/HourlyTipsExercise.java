package com.ververica.flinktraining.exercises.datastream_java.windows;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3; 
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

  public static void main(String[] args) throws Exception {

    // Чтение параметров из командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    // Получение пути к входному файлу с данными о плате за такси
    final String input = params.get("input", ExerciseBase.pathToFareData);
  
    final int maxEventDelay = 60; // Максимальная задержка событий в секундах 
    final int servingSpeedFactor = 600; // Фактор скорости обработки 

    // Настройка окружения для потоковой обработки данных
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(ExerciseBase.parallelism);

    // Создание потока данных о плате за такси из источника данных
    DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

    // Расчет почасовых чаевых для каждого водителя
    DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
        .keyBy((TaxiFare fare) -> fare.driverId)
        .timeWindow(Time.hours(1))
        .process(new AddTips())
        .timeWindowAll(Time.hours(1))
        .maxBy(2);

    printOrTest(hourlyMax);

    env.execute("Hourly Tips (java)");
  }

  // Расчет суммы чаевых для каждого водителя в каждом временном окне
  public static class AddTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> { 

    @Override
    public void process( Long key, Context context,  Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out) throws Exception { 

      // Переменная для хранения суммы чаевых
      Float sumOfTips = 0F;

      // Итерация по всем записям о плате за такси в текущем окне
      for (TaxiFare f : fares) {
        // Добавление суммы чаевых из текущей записи к общей сумме
        sumOfTips += f.tip;
      }

      out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
    }
  }
}
