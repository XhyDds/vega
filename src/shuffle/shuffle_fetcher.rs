use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::{atomic, atomic::AtomicBool, Arc};

use crate::env;
use crate::serializable_traits::Data;
use crate::shuffle::*;
use futures::future;
use hyper::{client::Client, Uri};
use tokio::sync::Mutex;

/// Parallel shuffle fetcher.
pub(crate) struct ShuffleFetcher;

impl ShuffleFetcher {
    pub async fn fetch<K: Data, V: Data>(
        //Data是trait
        shuffle_id: usize,
        reduce_id: usize,
    ) -> Result<impl Iterator<Item = (K, V)>> {
        //该函数并行地从多个服务器上的shuffle文件中读入数据（数量为分区数*reduce任务数，路径为`shuffle_uri/input_id/reduce_id`），并返回反序列化后的结果数组迭代器
        //该函数的输入参数包括shuffle_id和reduce_id，这两个参数用于确定要获取的shuffle数据的uri：
        //一个shuffle_id对应多个uri，一个uri对应多个index（意义是input_id），再从路径`shuffle_uri/input_id/reduce_id`读入数据

        /*
            其执行过程如下：
            1. 我们知道，一个shuffle_id对应的任务分成多部分散布在各机器上执行，每台机器有URI，一台机器同时执行多个部分，按照shuffle_id获取其对应的URI得一数组，又按URI对数组下标进行分组，得到uri->[index1,index2,...]的（K,V）对。完成后，将这些（K,V）对装入队列。
            2. 然后，该函数为每个服务器URI生成一个异步任务。每个异步任务都会从服务器队列中获取某URI指定的input_id（即原来的index元组），并令HTTP客户端从shuffle_uri/input_id/reduce_id获取数据，加进shuffle_chunks里面并返回
            3. 合并所有异步任务结果
        */
        log::debug!("inside fetch function");
        let mut inputs_by_uri = HashMap::new();
        //首先根据shuffle_id，获取对应的服务器上的URI列表
        let server_uris = env::Env::get()
            .map_output_tracker
            .get_server_uris(shuffle_id)
            .await
            .map_err(|err| ShuffleError::FailFetchingShuffleUris {
                source: Box::new(err),
            })?; //get server_uris: Vec<String>
        log::debug!(
            "server uris for shuffle id #{}: {:?}",
            shuffle_id,
            server_uris
        );
        //接下来，该函数将服务器URI对应服务器id打包成一个元组，并将它们添加到一个服务器队列中。
        for (index, server_uri) in server_uris.into_iter().enumerate() {
            inputs_by_uri
                .entry(server_uri) //注：entry是<K,V>的意思
                .or_insert_with(Vec::new)
                .push(index);
        } //这段代码等价于：向键值server_uri索引的Vec中插入index（若为空则插入一个空的Vec）
          // 那么现在我们获得了一个hashmap：inputs_by_uri，其映射关系是uri->index，即将server_uris标号0~len-1，按URI值将这些标号分组

        //所以，一个shuffle_task（以id辨识）对应多个shuffle_uri，每个shuffle_uri又因为分区等原因同时执行同一任务多部分

        let mut server_queue = Vec::new();
        let mut total_results = 0;
        for (key, value) in inputs_by_uri {
            total_results += value.len();
            server_queue.push((key, value));
        } //装填server_queue，按key遍历map，将每一对（K,V）（即uri->[index1,index2,...]装入队列）
        log::debug!(
            "servers for shuffle id #{:?} & reduce id #{}: {:?}",
            shuffle_id,
            reduce_id,
            server_queue
        );
        //从这里开始的代码段，可认为中间变量还有用的只剩下server_queue，其它变量已经完成了历史使命

        //然后，该函数为每个服务器URI生成一个异步任务，每个异步任务都会从服务器队列中获取某URI指定的input_id（即原来的index元组），
        //并令HTTP客户端从shuffle_uri/input_id/reduce_id获取数据，加进shuffle_chunks里面
        let num_tasks = server_queue.len(); //uri数
        let server_queue = Arc::new(Mutex::new(server_queue));
        let failure = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::with_capacity(num_tasks);
        for _ in 0..num_tasks {
            let server_queue = server_queue.clone();
            let failure = failure.clone();
            // spawn a future for each expected result set
            let task = async move {
                //每个异步任务都会从服务器队列中获取某URI指定的index元组，并使用HTTP客户端从服务器中获取数据
                let client = Client::builder().http2_only(true).build_http::<Body>();
                let mut lock = server_queue.lock().await;
                if let Some((server_uri, input_ids)) = lock.pop() {
                    //从队列中取出一个(K, [index1,index2,...])
                    let server_uri = format!("{}/shuffle/{}", server_uri, shuffle_id);
                    let mut chunk_uri_str = String::with_capacity(server_uri.len() + 12);
                    chunk_uri_str.push_str(&server_uri); //String类型的server_uri
                    let mut shuffle_chunks = Vec::with_capacity(input_ids.len());
                    for input_id in &input_ids[0..1] {
                        //changed for sort_shuffle
                        //URI对应的index1,index2,...，即分区号
                        if failure.load(atomic::Ordering::Acquire) {
                            // Abort early since the work failed in an other future
                            return Err(ShuffleError::Other);
                        }
                        log::debug!("inside parallel fetch {}", input_id);
                        let chunk_uri = ShuffleFetcher::make_chunk_uri(
                            &server_uri,
                            &mut chunk_uri_str,
                            *input_id, //changed for sort_shuffle
                            reduce_id,
                        )?; //这个文件是以input_id和reduce_id命名的！1个shuffle task会产生分区数*reduce任务数个文件
                        let data_bytes = {
                            let res = client.get(chunk_uri).await?; //这里get会把shuffle文件给读出来
                            hyper::body::to_bytes(res.into_body()).await
                        }; //通过http连接从服务器获取数据
                           //如果获取数据成功，则将其解析为(K, V)元组，并将它们添加到shuffle_chunks向量中。
                        if let Ok(bytes) = data_bytes {
                            let deser_vec_data =
                                bincode::deserialize::<Vec<Vec<u8>>>(&bytes.to_vec())?;
                            for data in deser_vec_data {
                                let deser_data = bincode::deserialize::<Vec<(K, V)>>(&data)?;
                                shuffle_chunks.push(deser_data); //向shuffle_chunks装入deser_data
                            }
                        } else {
                            failure.store(true, atomic::Ordering::Release);
                            return Err(ShuffleError::FailedFetchOp);
                        }
                    }
                    Ok::<Box<dyn Iterator<Item = (K, V)> + Send>, _>(Box::new(
                        shuffle_chunks.into_iter().flatten(), //flatten函数令多层嵌套的数组层数减一（输入输出都是迭代器）
                    ))
                } else {
                    Ok::<Box<dyn Iterator<Item = (K, V)> + Send>, _>(Box::new(std::iter::empty()))
                }
            };
            tasks.push(tokio::spawn(task));
        }
        //最后一步，合并所有异步任务结果
        log::debug!("total_results fetch results: {}", total_results); //结果数
        let task_results = future::join_all(tasks.into_iter()).await;
        let results = task_results.into_iter().fold(
            Ok(Vec::<(K, V)>::with_capacity(total_results)),
            |curr, res| {
                if let Ok(mut curr) = curr {
                    if let Ok(Ok(res)) = res {
                        curr.extend(res); //extend函数功能：将res迭代器指向的集合里每个元素按元素顺序附加到curr后面
                        Ok(curr)
                    } else {
                        Err(ShuffleError::FailedFetchOp)
                    }
                } else {
                    Err(ShuffleError::FailedFetchOp)
                }
            },
        )?; //此句功能是合并所有结果到curr这个Vec中

        /* 注：fold函数原型为：fn fold<B, F>(self, init: B, f: F) -> B，它从左到右对数组里每个元素v应用init=f(init,v)
        如此示例：以下代码是对数组求和：
            let a = [1, 2, 3];
            let sum = a.iter().fold(0, |acc, x| acc + x);
            assert_eq!(sum, 6);
         */
        Ok(results.into_iter())
    }

    fn make_chunk_uri(
        base: &str,
        chunk: &mut String,
        input_id: usize,
        reduce_id: usize,
    ) -> Result<Uri> {
        /*
        函数功能：按以下方式处理uri字符串
        base -> base/input_id/reduce_id
        */
        let input_id = input_id.to_string();
        let reduce_id = reduce_id.to_string();
        let path_tail = ["/".to_string(), input_id, "/".to_string(), reduce_id].concat();
        if chunk.len() == base.len() {
            chunk.push_str(&path_tail);
        } else {
            chunk.replace_range(base.len().., &path_tail); //base后面已经跟了tail了
        }
        Ok(Uri::try_from(chunk.as_str())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[tokio::test(core_threads = 4)]
    // async fn fetch_ok() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
    //     {
    //         let addr = format!(
    //             "http://127.0.0.1:{}",
    //             env::Env::get().shuffle_manager.server_port
    //         );
    //         let servers = &env::Env::get().map_output_tracker.server_uris;
    //         servers.insert(11000, vec![Some(addr)]);

    //         let data = vec![(0i32, "example data".to_string())];
    //         let serialized_data = bincode::serialize(&data).unwrap();
    //         //env::SHUFFLE_CACHE.insert((11000, 0, 11001), serialized_data);
    //         env::SHUFFLE_CACHE.insert((11000, 11001), vec![serialized_data]);
    //     }

    //     let result: Vec<(i32, String)> = ShuffleFetcher::fetch(11000, 11001)
    //         .await?
    //         .into_iter()
    //         .collect();
    //     assert_eq!(result[0].0, 0);
    //     assert_eq!(result[0].1, "example data");

    //     Ok(())
    // }

    #[tokio::test(core_threads = 4)]
    async fn fetch_failure() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        {
            let addr = format!(
                "http://127.0.0.1:{}",
                env::Env::get().shuffle_manager.server_port
            );
            let servers = &env::Env::get().map_output_tracker.server_uris;
            servers.insert(10000, vec![Some(addr)]);

            let data = "corrupted data";
            let serialized_data = bincode::serialize(&data).unwrap();
            //env::SHUFFLE_CACHE.insert((10000, 0, 10001), serialized_data);
            env::SHUFFLE_CACHE.insert((10000, 10001), vec![serialized_data]);
        }

        let err = ShuffleFetcher::fetch::<i32, String>(10000, 10001).await;
        assert!(err.is_err());

        Ok(())
    }

    #[test]
    fn build_shuffle_id_uri() -> StdResult<(), Box<dyn std::error::Error + 'static>> {
        let base = "http://127.0.0.1/shuffle";
        let mut chunk = base.to_owned();

        let uri0 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 0, 1)?;
        let expected = format!("{}/0/1", base);
        assert_eq!(expected.as_str(), uri0);

        let uri1 = ShuffleFetcher::make_chunk_uri(base, &mut chunk, 123, 123)?;
        let expected = format!("{}/123/123", base);
        assert_eq!(expected.as_str(), uri1);

        Ok(())
    }
}
