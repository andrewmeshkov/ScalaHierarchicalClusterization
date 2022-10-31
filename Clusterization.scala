import scala.util.control.Breaks.{break, breakable}

object Clusterization {
  def main(args: Array[String]): Unit = {
    val test = get_arr("")//здесь просто вставить имя файла
    HASLC(test)
  }

  def norm(pnt1 : Array[Double], pnt2 : Array[Double]): Double ={
    var dist : Double = 0
    for(i <- pnt1.indices){
      dist += math.pow(math.abs(pnt1(i) - pnt2(i)), 2)
    }
    return math.sqrt(dist)
  }

  // расстояние между кластерами = минимальное расстояние между точками

  def distance_between_clusters(clst1 : Array[Array[Double]], clst2: Array[Array[Double]]): Double ={
    var dist = Array[Double]()
    for(i <- clst1.indices){
      for(j <- clst2.indices){
        dist = dist :+ norm(clst1(i), clst2(j))
      }
    }
    return dist.min
  }
  //получение массива для кластеризации из csv-файла
  def get_arr(str : String) : Array[Array[Array[Double]]] = {
    val bufferedSource = io.Source.fromFile(str)
    var matrix :Array[Array[Array[Double]]] = Array.empty
    var temp: Array[Array[Double]] = Array.empty
    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim.toDouble)
      temp = temp :+ cols
      matrix = matrix :+ temp
      temp = Array.empty
    }
    bufferedSource.close
    return matrix
  }

  //создание матрицы заполненой нулями

  def init (r: Int) = Array.fill(r, r)( 0.0 )

  //функция кластеризации (hierarchical agglomerative single-linkage clusterization)

  def HASLC(clusters : Array[Array[Array[Double]]]): Unit={
    var temp : Array[Array[Array[Double]]] = clusters.clone()
    var lng = temp.length
    while (lng > 0) {
      //вывод элементов кластеров
      for(cluster <- temp.indices){
        println(s"cluster №${cluster+1}")
        for(obj <- temp(cluster).indices){
          for(coord <- temp(cluster)(obj).indices){
            print(s"par. №${coord+1} : ${temp(cluster)(obj)(coord)} ")
          }
          println()
        }
      }
      println("------------------------------------")
      // создание матрицы расстояний
      var sim_matrix = init(lng)
      for (i <- 0 until lng) {
        for (j <- 0 until lng) {
          val dist = distance_between_clusters(temp(i), temp(j))
          sim_matrix(i)(j) = dist
        }
      }
      // поиск минимального расстояния между объектами
      var min_dist = Double.MaxValue
      for (i <- 0 until lng) {
        for (j <- 0 until lng) {
          if (j > i) {
            min_dist = Math.min(sim_matrix(i)(j), min_dist)
          }
        }
      }
      if (lng > 1) {
        var x = 0;
        var y = 0
        //определение двух точек с минимальным расстоянием между ними
        breakable {
          for (i <- 0 until lng) {
            for (j <- 0 until lng) {
              x = i
              y = j
              if (j > i && sim_matrix(i)(j) == min_dist) {
                break
              }
            }
            break
          }
        }
        //объединение кластеров
        var new_clusters: Array[Array[Array[Double]]] = Array[Array[Array[Double]]]()
        for (ind <- 0 until lng) {
          if (ind != x && ind != y) {
            new_clusters = new_clusters :+ temp(ind)
          }
        }
        new_clusters = new_clusters :+ (temp(x) ++ temp(y))
        temp = new_clusters
      }
      lng = lng - 1
    }
  }

}
