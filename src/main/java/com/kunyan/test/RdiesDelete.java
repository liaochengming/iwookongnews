package com.kunyan.test;

import de.mwvb.base.xml.XMLDocument;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Iterator;
import java.util.Set;

public class RdiesDelete {

    public static void main(String[] args) {

        XMLDocument doc = new XMLDocument();
        doc.loadFile(args[0]);
        String host = doc.selectSingleNode("xml/redis/host").getText();
        String port = doc.selectSingleNode("xml/redis/port").getText();
        String auth = doc.selectSingleNode("xml/redis/auth").getText();
        String db = doc.selectSingleNode("xml/redis/db").getText();
        // 池基本配置
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(10);
        config.setMaxWaitMillis(10000);
        config.setTestOnBorrow(false);

        JedisPool jedisPool = new JedisPool(config, host,
                Integer.valueOf(port),
                20000, auth,
                Integer.valueOf(db));

        Jedis jedis = jedisPool.getResource();
        Set<String> set = jedis.keys(args[1] + "*");

        Iterator it = set.iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            jedis.del(key);
            System.out.println(key);
        }

    }
}
