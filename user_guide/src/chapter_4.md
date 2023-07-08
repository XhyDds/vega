# HDFS适配指南
## HDFS的安装与环境配置

## 配置文件的设置
如果需要使用HDFS，请在hosts.conf文件中配置namenode参数，并为每个slave节点设置对应的java_home与hadoop_home参数。
其中，namenode代指hdfs的主节点ip(不需要添加端口)，java_home代指java的安装路径，hadoop_home代指hadoop的安装路径。
参考文件如[hosts.conf](../../config_files/hosts.conf)所示。

## 相关RDD的使用
