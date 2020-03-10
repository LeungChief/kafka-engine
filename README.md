> ### 前言


#### &emsp;&emsp;对kafka-python进行了封装的类文件.用于项目的分布式数据收集、统一配置数据、持久化功能<br>
#### &emsp;&emsp;目前仅完成一些基础类,计划日后会逐步完善<br>

<br>

> ### 环境需求

##### &emsp;&emsp;java 1.8.0
##### &emsp;&emsp;zookeeper 3.5.6
##### &emsp;&emsp;kafka 2.0.2 
##### &emsp;&emsp;mongodb 4.2.3 
<br>
你需要通过 pip 安装以下依赖 <br>
* kafka-python<br>
* pymongo
<br>
<br>

> ### 版本信息

##### &emsp;&emsp;v0.01 实现了用database管理kafka消费端的offset,并且可回滚消费端超时前的offset

> ### 使用
#### `以下代码是假定你已熟悉kafka-python的参数配置,并且已配置好环境`

##### 创建topic:
<pre>

topicList = [
        {'name': 'test_topic', 'num_partitions': 2, 'replication_factor': 3, 'replica_assignments': {}, 'topic_configs': {}}
    ]

AdminClient().create_topics(topic_list=topicList)
</pre>

##### 删除topic:
<pre>
AdminClient().delete_topics(topic=['test_topic','test_topic2','test_topic3'])
</pre>

##### 获取当前用户有权查看的topic:
<pre>
Consumer().get_user_topics()
</pre>

##### 生产端:
<pre>
Product().product_send(topic='new_topic', key=b'key', value=b'value')
</pre>

##### 消费端:
<pre>
message = Consumer(group_id='test')
              .assign_partition(
            [{'topic': 'new_topic', 'partition': 1}, {'topic': 'topicnewtest1', 'partition': 0}]).topic_consumer()

print(message)
</pre>

> ### 参考资料

https://s0kafka-python0readthedocs0io.icopy.site/en/master/usage.html