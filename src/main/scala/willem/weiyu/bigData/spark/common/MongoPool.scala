package willem.weiyu.bigData.spark.common

import java.util

import com.mongodb.{MongoClient, ServerAddress}

/**
  * @Author weiyu005
  * @Description
  * @Date 2018/12/3 19:57
  */
object MongoPool {
  var instances = Map[String, MongoClient]()

  def nodes2ServerList(nodes: String): java.util.List[ServerAddress] = {
    val serverList = new util.ArrayList[ServerAddress]()
    nodes.split(",").map(portNode => portNode.split(":"))
      .flatMap(ar => {
        if (ar.length == 2) {
          Some(ar(0), ar(1).toInt)
        } else {
          None
        }
      }).foreach({ case (node, port) => serverList.add(new ServerAddress(node, port)) })
    serverList
  }

  def apply(nodes:String): MongoClient = {
    instances.getOrElse(nodes,{
      val servers = nodes2ServerList(nodes)
      val client = new MongoClient(servers)
      instances += nodes -> client
      client
    })
  }
}
