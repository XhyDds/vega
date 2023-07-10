# HDFS适配指南
## HDFS的安装与环境配置
### 下载Hadoop
前往[Hadoop官网](https://hadoop.apache.org/releases.html)下载Hadoop。
也可在[Release页面](https://archive.apache.org/dist/hadoop/common/)选择合适的版本下载。本项目编写时使用的版本为3.3.5。
建议下载预编译版(即较大的.tar.gz文件)，以下的所有内容均从已编译好的Hadoop开始。若要从源代码开始编译Hadoop，请自行查找其他教程。
主机和从机上都要安装Hadoop。可以考虑主机下载好且配置好免密登录后，用scp命令分发。
### 配置免密登录
将所有节点的公钥加入每台机器的`.ssh/authorized_keys`文件末尾即可。
上述操作会使得任意两台主机间（包括到自己）都可以免密登录。但理论上免密登录是用于Hdfs的启动和关闭脚本，因此只配置主机和从机之间的免密登录应该也可用。用户可自行尝试。
### 安装Java
Hadoop需要的Java版本为JDK1.8，我们开发时测试能用的版本为JDK1.8.0_371。可自行从Java官网下载对应版本安装包安装。
### 配置环境变量
编辑`/etc/profile`或`~/bashrc`。
首先在末尾加入`JAVA_HOME`和`HADOOP_HOME`，内容分别为JAVA和HADOOP的安装路径。
然后加入
```bash
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
export CLASSPATH=${HADOOP_HOME}/etc/hadoop:`find ${HADOOP_HOME}/share/hadoop/ | awk '{path=path":"$0}END{print path}'`
export LD_LIBRARY_PATH="${HADOOP_HOME}/lib/native":$LD_LIBRARY_PATH
export PDSH_RCMD_TYPE=ssh
```
这样的配置每个节点都要做。
### 配置Hadoop
首先，在Hadoop目录中的`etc/hadoop/hadoop-env.sh`文件中加入`export JAVA_HOME=[Java路径]`，与配置环境变量时结构相同。
然后修改`etc/hadoop/workers`，将所有worker节点的IP加入其中，每个节点一行。
之后还需修改`etc/hadoop/`下的`core-site.xml`和`hdfs-site.xml`文件，指定NameNode地址等参数。具体内容可以参考其他教程。
最后将
```
HDFS_DATANODE_USER=aaa
HDFS_DATANODE_SECURE_USER=bbb
HDFS_NAMENODE_USER=ccc
HDFS_SECONDARYNAMENODE_USER=ddd
```
这几个参数加入`sbin/`目录中的`start-dfs.sh`，`stop-dfs.sh`文件顶端，这几个参数的意义即字面意义。aaa、bbb等字段需替换成启动时用来ssh登录的用户（即配置了免密登录的用户）。
上面的这些配置，每一个节点上的Hadoop都需配置。如果节点结构相同，可以先配好一个节点的内容再用scp命令分发。
最后在主节点的控制台输入并运行指令`hdfs namenode -format`，格式化NameNode。
### 运行测试
上面的配置都完成后，在HDFS的主节点终端上输入`start-dfs.sh`即可启动HDFS。输入`stop-dfs.sh`即可关闭HDFS。可以用`hdfs dfs -ls`，`hdfs dfs -put`，`hdfs dfs -get`等终端命令操作HDFS。还可通过浏览器访问`master:9870/`页面查看HDFS的运行情况。其中`master`应替换为HDFS主节点的IP地址。


## 配置文件的设置
如果需要使用HDFS，请在hosts.conf文件中配置namenode参数，并为每个slave节点设置对应的java_home与hadoop_home参数。
其中，namenode代指hdfs的主节点ip(不需要添加端口)，java_home代指java的安装路径，hadoop_home代指hadoop的安装路径。
参考文件如[hosts.conf](../../config_files/hosts.conf)所示。


## 相关类的使用
### HdfsIO
与HDFS之间的交互通过`HdfsIO`类完成。
要使用这个类提供的各种功能，需要首先调用`new()`方法生成对象。该类提供了以下几个实例方法：
#### read_to_vec
```rust
pub fn read_to_vec(&mut self, path: &str) -> Result<Vec<u8>>
```
该方法读取路径path对应的文件。若读取失败则返回对应的Err，否则返回Ok，内容为读取得到的字节向量。
该方法用来处理需要直接使用文件内容的一些特殊情况，如要将文件内容放入Rdd进行计算，建议尽可能使用`read_to_rdd`或`read_to_rdd_and_decode`
#### read_to_rdd
```rust
pub fn read_to_rdd(
        &mut self,
        path: &str,
        context: &Arc<Context>,
        num_slices: usize,
    ) -> HdfsReadRdd
```
该方法返回路径为`path`，分区数为`num_slices`的`HdfsReadRdd`。
对HdfsReadRdd的介绍，详见[HdfsReadRdd](#hdfsreadrdd)
#### read_to_rdd_and_decode
```rust
pub fn read_to_rdd_and_decode<U, F>(
        &mut self,
        path: &str,
        context: &Arc<Context>,
        num_slices: usize,
        decoder: F,
    ) -> SerArc<dyn Rdd<Item = U>>
    where
        F: SerFunc(Vec<u8>) -> U,
        U: Data,
```
该方法返回的实际为一个MapperRdd，map操作中的函数为一个对字节向量进行解码的函数`decoder`。`Decoders`类中预置了一些常用的`decoder`，详见对`Decoders`类的介绍。
#### write_to_hdfs
```rust
pub fn write_to_hdfs (&mut self, data: &[u8], path: &str, create: bool) -> Result<()>
```
将`&[u8]`类型数据写入hdfs的指定路径中。返回的结果用来表示写入是否成功。
注意：该方法不会递归地创建需要的目录，也不会覆写已存在的同名文件。

## HdfsReadRdd
该Rdd仅由`read_to_rdd`方法返回，不能直接new。
文件读取到Rdd的功能主要依靠`HdfsReadRdd`来实现。该Rdd在创建时接收路径和namenode等信息，随后会自动判断路径是文件还是文件夹，并按照指定的分区数对所有文件进行分区（若分区数大于文件总数，则令分区数等于文件数）。
该Rdd对应的Item类型为`Vec<u8>`，即字节向量。即每个分区可以看作是内容为字节向量的迭代器。因此map等操作应对字节向量进行。
另外，创建Rdd时仅会读取元数据进行分区操作。实际文件内容的读取在计算阶段进行。

### Decoders
内置2个类方法，各返回一个用于解码的函数。
#### to_utf8()
```rust
pub fn to_utf8() -> impl SerFunc(Vec<u8>) -> String
```
将字节向量按utf8编码解码为`String`。即每个文件对应一整个`String`。
#### to_utf8_lines()
```rust
pub fn to_utf8_lines() -> impl SerFunc(Vec<u8>) -> Vec<String>
```
将字节向量按utf8编码解码，并按行分割为字符串，每个文件对应一个内容为字符串的向量。