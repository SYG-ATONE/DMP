package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtils  {
   val conf:JedisPoolConfig = new JedisPoolConfig()

    conf.setMaxTotal(30)
    conf.setMaxIdle(10)

   val pool = new JedisPool(conf,"hadoop02",6379)

  def getConnection(): Jedis={
    pool.getResource
  }
}