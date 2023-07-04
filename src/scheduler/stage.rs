use crate::dependency::ShuffleDependencyTrait;
use crate::rdd::RddBase;
use std::cmp::Ordering;
use std::fmt::Display;
use std::sync::Arc;

/**
 * 结构体：Stage
 * 描述：Spark中的Stage，按id升序排序
 * 成员：
 * id: usize，stage的id
 * num_partitions: usize，stage的分区数
 * shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>，shuffle依赖
 * is_shuffle_map: bool，是否是shuffleMapStage
 * rdd: Arc<dyn RddBase>，对应的rdd
 * parents: Vec<Stage>，父stage
 * output_locs: Vec<Vec<String>>，输出位置
 * num_available_outputs: usize，可用的输出节点数
 */
#[derive(Clone)]
pub(crate) struct Stage {
    pub id: usize,
    pub num_partitions: usize,
    pub shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>, //所依赖的shuffle
    pub is_shuffle_map: bool,
    pub rdd: Arc<dyn RddBase>,
    pub parents: Vec<Stage>,
    pub output_locs: Vec<Vec<String>>,
    pub num_available_outputs: usize,
}

impl PartialOrd for Stage {
    fn partial_cmp(&self, other: &Stage) -> Option<Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl PartialEq for Stage {
    fn eq(&self, other: &Stage) -> bool {
        self.id == other.id
    }
}

impl Eq for Stage {}

impl Ord for Stage {
    fn cmp(&self, other: &Stage) -> Ordering {
        self.id.cmp(&other.id)
    }
}
impl Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Stage {}", self.id)
    }
}
impl Stage {
    pub fn get_rdd(&self) -> Arc<dyn RddBase> {
        self.rdd.clone()
    }

    pub fn new(
        id: usize,
        rdd: Arc<dyn RddBase>,
        shuffle_dependency: Option<Arc<dyn ShuffleDependencyTrait>>,
        parents: Vec<Stage>,
    ) -> Self {
        Stage {
            id,
            num_partitions: rdd.number_of_splits(),
            is_shuffle_map: shuffle_dependency.clone().is_some(),
            shuffle_dependency,
            parents,
            rdd: rdd.clone(),
            output_locs: {
                let mut v = Vec::new();
                for _ in 0..rdd.number_of_splits() {
                    v.push(Vec::new());
                }
                v
            },
            num_available_outputs: 0,
        }
    }

    /// 父stage为空，且当前非shuffleMapStage则可进行
    pub fn is_available(&self) -> bool {
        if self.parents.is_empty() && !self.is_shuffle_map {
            true
        } else {
            log::debug!(
                "num available outputs {}, and num partitions {}, in is available method in stage",
                self.num_available_outputs,
                self.num_partitions
            );
            self.num_available_outputs == self.num_partitions
        }
    }

    pub fn add_output_loc(&mut self, partition: usize, host: String) {
        log::debug!(
            "adding loc for partition inside stage {} @{}",
            partition,
            host
        );
        // 若当前partition的输出节点非空，则可用输出节点数加一，即原本该partition无输出
        if !self.output_locs[partition].is_empty() {
            self.num_available_outputs += 1;
        }
        self.output_locs[partition].push(host);
    }

    //
    pub fn remove_output_loc(&mut self, partition: usize, host: &str) {
        let prev_vec = self.output_locs[partition].clone();
        // new_vec为prev_vec中除host外的所有元素
        let new_vec = prev_vec
            .clone()
            .into_iter()
            .filter(|x| x != host)
            .collect::<Vec<_>>();
        // 若原来的prev_vec非空，且new_vec为空，则可用输出节点数减一，即当前partition无输出
        if (!prev_vec.is_empty()) && (new_vec.is_empty()) {
            self.num_available_outputs -= 1;
        }
        self.output_locs[partition] = new_vec;
    }
}
