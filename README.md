# vega

Previously known as `native_spark`.

[![Join the chat at https://gitter.im/fast_spark/community](https://badges.gitter.im/fast_spark/community.svg)](https://gitter.im/fast_spark/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/rajasekarv/native_spark.svg?branch=master)](https://travis-ci.org/rajasekarv/native_spark)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**[Documentation](https://rajasekarv.github.io/vega/)**

A new, arguably faster, implementation of Apache Spark from scratch in Rust. WIP

Framework tested only on Linux, requires nightly Rust. Read how to get started in the [documentation](https://rajasekarv.github.io/vega/chapter_1.html).

## Contributing

If you are interested in contributing please jump on Gitter and check out and head to the issue tracker where you can find open issues (anything labelled `good first issue` and `help wanted` is up for grabs).

## Updating(By ThisLynx)
作为课程大作业，本小组对该项目进行了维护，修正了部分代码，提升了容错性和运行效率，增加了与Hdfs、Prometheus、Grafana等的接口。

单机配置流程见文档 [help_loc](./user_guide/src/chapter_2.md).
多机配置流程见文档 [help_dis](./user_guide/src/chapter_3.md).
HDFS的接入流程见文档 [hdfs](./user_guide/src/chapter_4.md).
基于prometheus和grafana的性能监视方法见文档 [monitor](./user_guide/src/chapter_5.md).
