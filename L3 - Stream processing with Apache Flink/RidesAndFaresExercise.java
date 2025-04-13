package com.ververica.flinktraining.exercises.datastream_java.state;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;


public class RidesAndFaresExercise extends ExerciseBase
{
  public static void main(String[] args) throws Exception
  {
    // Получение параметров из командной строки
    ParameterTool params = ParameterTool.fromArgs(args);
    // Получение пути к файлу с данными о поездках
    final String ridesFile = params.get("rides", pathToRideData);
    // Получение пути к файлу с данными об оплатах
    final String faresFile = params.get("fares", pathToFareData);
    
    final int delay = 60;    // Максимальная задержка событий
    final int servingSpeedFactor = 1800;  // Фактор скорости обработки событий 

    // Настройка окружение потоковой обработки
    Configuration conf = new Configuration();
    conf.setString("state.backend", "filesystem"); 
    conf.setString("state.savepoints.dir", "file:///tmp/savepoints"); 
    conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints"); 
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf); // Создание локального окружения с веб-интерфейсом
    env.setParallelism(ExerciseBase.parallelism); 
    env.enableCheckpointing(10000L); // Включение checkpointing 
    CheckpointConfig config = env.getCheckpointConfig();
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); 

    // Создание потока данных о поездках
    DataStream<TaxiRide> rides = env
        .addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor))) 
        .filter((TaxiRide ride) -> ride.isStart) 
        .keyBy(ride -> ride.rideId); 

    // Создание потока данных об оплатах
    DataStream<TaxiFare> fares = env
        .addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor))) 
        .keyBy(fare -> fare.rideId); 

    // Установка UID для stateful flatmap operator
    DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
        .connect(fares) 
        .flatMap(new EnrichmentFunction()) 
        .uid("enrichment"); 

    printOrTest(enrichedRides); 
    env.execute("Join Rides with Fares (java RichCoFlatMap)"); 
  }

  public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>>
  {
    private ValueState<TaxiRide> rideState; 
    private ValueState<TaxiFare> fareState; 

    @Override
    public void open(Configuration config)
    {
      rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class)); 
      fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class)); 
    }

    // Обработка TaxiRide
    @Override
    public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception
    {
      TaxiFare fare = fareState.value(); // Получение оплаты из состояния
      if (fare != null)
      {
        fareState.clear(); 
        out.collect(new Tuple2(ride, fare)); // Отправка объединенных данных
      } else
      {
        rideState.update(ride); // Сохранение поездки в состояние
      }
    }

    // Обработка TaxiFare
    @Override
    public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception
    {
      TaxiRide ride = rideState.value(); // Получение поездки
      if (ride != null)
      {
        rideState.clear(); 
        out.collect(new Tuple2(ride, fare)); 
      } else
      {
        fareState.update(fare); 
      }
    }
  }
}
