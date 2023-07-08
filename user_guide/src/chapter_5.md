# 加入性能监控的方式

> 当前的默认写法为在Windows平台下Docker中部署时的情况，若在别的平台可能需要不同的配置，读者可以自行查找。

## 配置Prometheus

若需要监测远程的节点，在`docker/monitoring/prometheus.yml`中修改`job_name: 'node'`下的`targets`值，使用`ip:port`模式，应能指向对应节点上的node_exporter的端口。同样，`job_name: 'vega'`下的`targets`值应指向vega的主节点上的8000端口(vega默认性能输出端口)。

## Docker

直接在docker/文件夹下使用命令

```bash
docker compose up -d
```

后在http://localhost:3000 打开grafana页面，使用用户名admin密码admin登录，即可在dashboards下查看到对应的监控面板。

## 远程手动

在远程服务器上执行以下命令

```bash
cd ~
wget https://github.com/prometheus/node_exporter/releases/download/v1.6.0/node_exporter-1.6.0.linux-amd64.tar.gz
tar -xzvf node_exporter-1.6.0.linux-amd64.tar.gz
cd node_exporter-1.6.0.linux-amd64
./node_exporter &
```

在本地下载prometheus和grafana或直接使用docker

下载命令如下

```bash
cd ~
wget https://dl.grafana.com/enterprise/release/grafana-enterprise-9.5.2.linux-amd64.tar.gz
tar -xzvf grafana-enterprise-9.5.2.linux-amd64.tar.gz
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar -xzvf prometheus-2.45.0.linux-amd64.tar.gz
```
