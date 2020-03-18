> ### 前言


#### &emsp;&emsp;对kafka-python进行了封装的类文件.用于项目的分布式数据收集、统一配置数据、持久化提供基类

<br>
<br>

> ### 环境需求

* 至少三台内存不少于1G的机子
* python 3.6或以上
* java 1.8.0
* zookeeper 3.5.6
* kafka 2.12-2.4.0 
* mongodb 4.2.3 
<br>

你需要通过 pip 安装以下依赖 
* kafka-python
* pymongo
<br>
<br>

> ### 版本信息

#### v0.01 实现了用database管理kafka消费端的offset,并且可回滚offset
<br>
<br>

> ### 使用

#### 创建topic:
<pre>

topicList = [
        {'name': 'test_topic', 'num_partitions': 2, 'replication_factor': 3, 'replica_assignments': {}, 'topic_configs': {}}
    ]

AdminClient().create_topics(topic_list=topicList)
</pre>

#### 删除topic:
<pre>
AdminClient().delete_topics(topic=['test_topic','test_topic2','test_topic3'])
</pre>

#### 获取当前用户有权查看的topic:
<pre>
Consumer().get_user_topics()
</pre>

#### 生产端:
<pre>
Product().product_send(topic='new_topic', key=b'key', value=b'value')
</pre>

Product()可传入初始化kafka客户端的配置参数.一般权重较低的参数可直接在配置文件中配置.客户端会自动调用<br>
例如:
<pre>
# bootstrap_servers不填默认为localhost:9092
bootstrap_servers=['1.1.1.1:9092,1.1.1.2:9092,1.1.1.3:9092']
value_serializer=lambda m: json.dumps(m).encode('ascii') 

Product().product_send(bootstrap_servers=bootstrap_servers,
                        value_serializer=value_serializer,
                        topic='new_topic', 
                        key=b'key', 
                        value=b'value')
</pre>
设置client_id
<pre>
Product().product_send(topic='new_topic', key=b'key', value=b'value').set_clientId("me")
</pre>

<br>

#### 消费端:
<pre>
message = Consumer(group_id='test')
              .assign_partition(
            [{'topic': 'new_topic', 'partition': 1}, {'topic': 'topicnewtest1', 'partition': 0}]).topic_consumer()

print(message)
</pre>
消费端的参数配置方式同客户端一样,提供了2种消费模式.设置client_id的方式也是一样<br>
 1、手动分配分区给消费者<br>
 2、消费者订阅指定分区(待完善)<br>
上面的代码就是模式1<br>

message是一个KafkaConsumer可迭代对象.


<br>
<br>

> ### 参考资料

https://s0kafka-python0readthedocs0io.icopy.site/en/master/usage.html