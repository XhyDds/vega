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
As a course project, our team has maintained and improved the project by fixing some code, enhancing fault tolerance and runtime efficiency, and adding interfaces with HDFS, Prometheus, Grafana, etc.

For the configuration process on a single machine, please refer to the document [help_loc](./user_guide/src/chapter_2.md). 
For the configuration process on multiple machines, please refer to the document [help_dis](./user_guide/src/chapter_3.md). 
The configuration process for integrating with HDFS is detailed in the document [hdfs](./user_guide/src/chapter_4.md). 
The performance monitoring method based on Prometheus and Grafana is described in the document [monitor](./user_guide/src/chapter_5.md).

