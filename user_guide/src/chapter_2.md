## 单机部署指南
以下为单机部署vega的指南：

1. 推荐使用linux系统运行（windows系统在部分环节可能略有不同，需要自己摸索）
   
2. 关于rust的版本，请选择 rust nightly ，具体版本请参考这里：
   <img src="./imgs/1.png">
   <img src="./imgs/2.png">
   推荐rustup default stable（日常开发用稳定版），在vega目录下`rustup override set nightly`重载为nightly格式。

3. 如果出现`error: failed to run custom build command for `openssl-sys v*`类似的错误，请按照提示下载openssl即可：
```doc
    # On Ubuntu
    sudo apt-get install libssl-dev
    # On Arch Linux
    sudo pacman -S openssl
    # On Fedora
    sudo dnf install openssl-devel
```

4. 注意，Rust运行需要完整的编译环境，安装 GCC 或 Clang，Ubuntu 系统下可以通过安装 build-essential 包完成。如果出现`error: failed to run custom build command for `ccl-sys v*`类似的错误，请检查是否安装了 GCC 或 Clang 。

5. 在家目录下(`echo $HOME`)，创建hosts.conf文件，内容格式同[config](../../config_files/hosts.conf)，参考的内容为：
```
master = "<host_ip>:8080"
# 请将<host_ip>替换成本机ip地址
```
   
6. 需要安装capnpc：
```doc
    curl -O https://capnproto.org/capnproto-c++-0.7.0.tar.gz
    tar zxf capnproto-c++-0.7.0.tar.gz
    cd capnproto-c++-0.7.0
```

7. 通过`cargo run`（main.rs文件已添加，为[wordcount](../../src/benchmark/wordcount.rs)内容）。更多样例请见[example](../../examples/)与[benchmark](../../src/benchmark/)

## 条件编译使用
在Cargo.toml
```
[features]
default=["hdrs_valid"]
hdrs_valid=[]
# aws_connectors = ["rusoto_core", "rusoto_s3"]
```
在default前加'#'表示注释，使得条件编译生效，忽略hdrs的编译.
去除'#'表示有hdrs_valid可用，条件编译会使得hdrs相关的模块正常编译.
