
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.{:+, io}
import scala.math.Ordering.{Double, Tuple3, comparatorToOrdering, ordered}
import scala.util.control.Breaks.{break, breakable}

object Spark_HSLAC {

  implicit def ordering[A <: Double]: Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = {
      x.compareTo(y)
    }
  }

  def HASLC(df: DataFrame, clusters : Int): Unit = {
    var temp = df.rdd
    var lng = temp.count()

    while (lng > 0) {

      //создание матрицы расстояний
      var sim_matrix = temp.cartesian(temp).map((x) => ((x._1.toSeq.toArray
        , x._2.toSeq.toArray), norm(x._1.toSeq.toArray.map(_.toString.toDouble),
        x._2.toSeq.toArray.map(_.toString.toDouble))))

      if (clusters > sim_matrix.count() && lng > 1) {

        //поиск пары точек с минимальным расстоянием между ними
        val min_dist = sim_matrix.min()(Ordering[Double].on(x => x._2))

        //Соединение объектов в один кластер
        var new_clst = sim_matrix.filter(x => ((x._1._1 sameElements min_dist._1._1) || (x._1._1 sameElements min_dist._1._2))
            && !(x._1._2 sameElements min_dist._1._1) && !(x._1._2 sameElements min_dist._1._2))
              .map(x => ((x._1._1, x._1._2), x._2)).reduceByKey(Array(_, _).minBy(x => x))

        new_clst = new_clst.union(new_clst.map(x => ((x._1._1, x._1._2), x._2)))

        //удаляем из изначальной матрицы те точки/кластеры , которые соединили в этой итерации и соединяем с  объектом , который представляет новосформированный кластер
        sim_matrix = sim_matrix
          .filter(x => !(x._1._1 sameElements min_dist._1._1) && !(x._1._2 sameElements min_dist._1._1)
            && !(x._1._1 sameElements min_dist._1._2) && !(x._1._2 sameElements min_dist._1._2))
          .union(new_clst).cache
      }
      else{
        break()
      }
      lng = lng - 1
    }

  }

  def norm(pnt1: Array[Double], pnt2: Array[Double]): Double =
    (pnt1, pnt2).zipped.map((x, y) => math.pow(math.abs(x - y), 2)).sum

  def main(args : Array[String]): Unit ={

    val spark = SparkSession.builder()
      .getOrCreate()

    val clusters = args(0).trim.toInt

    var df = spark.read.csv("") // просто вставить имя файла
    HASLC(df, clusters)
  }
}
