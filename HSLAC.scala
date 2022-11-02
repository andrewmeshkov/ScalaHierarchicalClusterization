
import scala.io
import scala.util.control.Breaks.{break, breakable}

object HSLAC {

  def get_arr(str: String): Array[Array[Array[Double]]] = {
    val bufferedSource = io.Source.fromFile(str)
    bufferedSource.getLines.toArray.map(x => x.trim.split(",").
        map(x => x.toDouble)).
      toArray.map(x => Array(x))
  }

  def HASLC(clusters: Array[Array[Array[Double]]]): Unit = {
    var temp: Array[Array[Array[Double]]] = clusters.clone()
    var lng = temp.length
    while (lng > 0) {
      //вывод элементов

      for (cluster <- temp.indices) {
        println(s"cluster №${cluster + 1}")
        for (obj <- temp(cluster).indices) {
          for (coord <- temp(cluster)(obj).indices) {
            print(s"par. №${coord + 1} : ${temp(cluster)(obj)(coord)} ")
          }
          println()
        }
      }

      // создание матрицы расстояний
      var sim_matrix = init(lng)
      for(i <- 0 until lng; j <- 0 until lng) sim_matrix(i)(j) = distance_between_clusters(temp(i), temp(j))

      // поиск минимального расстояния между объектами
      var min_distt = (for(i <- 0 until lng; j <- 0 until lng ) yield (sim_matrix(i)(j), i, j)).filter(x => x._1 != 0).minBy( x => x._1)

      if (lng > 1) {

        //объединение кластеров
        var new_clusters: Array[Array[Array[Double]]] = Array[Array[Array[Double]]]()
        (for(ind <- 0 until lng) yield ind).filter(ind => ind != min_distt._2 && ind != min_distt._3).foreach(ind => new_clusters = new_clusters :+ temp(ind))
        new_clusters = new_clusters :+ (temp(min_distt._2) ++ temp(min_distt._3))
        temp = new_clusters
      }
      lng = lng - 1
    }
  }

  def norm(pnt1: Array[Double], pnt2: Array[Double]): Double =
    (pnt1, pnt2).zipped.map((x, y) => math.pow(math.abs(x - y), 2)).sum

  // расстояние между кластерами = минимальное расстояние между точками

  def distance_between_clusters(clst1: Array[Array[Double]],
                                clst2: Array[Array[Double]]): Double =
    (for (p1 <- clst1; p2 <- clst2) yield norm(p1, p2)).min

  //создание матрицы заполненой нулями

  def init (r: Int) = Array.fill(r, r)( 0.0 )

  def main(args : Array[String]): Unit ={
    val test = get_arr("src/main/scala/test.scv") //здесь просто вставить имя файла
    HASLC(test)
  }
}
