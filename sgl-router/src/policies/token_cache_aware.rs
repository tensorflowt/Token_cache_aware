/*  
    Token Cache-Aware Load Balancing Router  
      
    This router combines load balancing with external cache matching:  
    1. Load Balancing (Shortest Queue with Balance Thresholds) - same as cache_aware  
    2. External Cache Matching via /v1/Nexuts/get_best_instance API  
      
    The router dynamically switches between these strategies based on load conditions.  
*/  
  
use std::{sync::Arc, time::Duration};  
use dashmap::DashMap;  
use rand::Rng;  
use tracing::{debug, info, warn};  
use serde::{Deserialize, Serialize};  
use tokio::sync::RwLock;  
use std::collections::HashMap;  
  
use super::{get_healthy_worker_indices, CacheAwareConfig, LoadBalancingPolicy};  
use crate::{core::Worker, metrics::RouterMetrics};  
use crate::tokenizer::traits::Tokenizer;  
  
// Nexuts API 请求和响应结构  
#[derive(Debug, Serialize)]  
struct NexutsRequest {  
    token_ids: Vec<u32>,  
}  
  
#[derive(Debug, Deserialize)]  
struct NexutsResponse {  
    worker_ids: Vec<String>,  
    confidence: f32,  
}  
  
/// Token Cache-aware routing policy  
pub struct TokenCacheAwarePolicy {  
    config: CacheAwareConfig,  
    tokenizer: Arc<dyn Tokenizer>,
    nexuts_url: String,  
    http_client: reqwest::Client,  
}  
  
impl TokenCacheAwarePolicy {  
    /// 创建新实例，需要传入 tokenizer  
    pub fn new(tokenizer: Arc<dyn Tokenizer>) -> Self {  
        Self::with_config_and_url(  
            CacheAwareConfig::default(),  
            "http://localhost:8080".to_string(),  
            tokenizer,  
        )  
    }  
  
    /// 使用配置和 tokenizer 创建实例  
    pub fn with_config(config: CacheAwareConfig, tokenizer: Arc<dyn Tokenizer>) -> Self {  
        Self::with_config_and_url(config, "http://localhost:8080".to_string(), tokenizer)  
    }  
  
    /// 使用配置、URL 和 tokenizer 创建实例  
    pub fn with_config_and_url(  
        config: CacheAwareConfig,  
        nexuts_url: String,  
        tokenizer: Arc<dyn Tokenizer>,  
    ) -> Self {  
        let http_client = reqwest::Client::builder()  
            .timeout(Duration::from_secs(5))  
            .build()  
            .expect("Failed to create HTTP client");  
  
        Self {  
            config,  
            tokenizer,    
            nexuts_url,  
            http_client,  
        }  
    }  
  
    /// 获取配置的不可变引用  
    pub fn config(&self) -> &CacheAwareConfig {  
        &self.config  
    }  
  
    /// 获取 Nexuts URL  
    pub fn nexuts_url(&self) -> &str {  
        &self.nexuts_url  
    }  
  
    /// 获取 tokenizer 引用  
    pub fn tokenizer(&self) -> &Arc<dyn Tokenizer> {  
        &self.tokenizer  
    }  
  
    /// 调用 Nexuts API 获取最佳 worker  
    async fn call_nexuts_api(&self, token_ids: Vec<u32>) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {  
        let request = NexutsRequest { token_ids };  
          
        let response = self.http_client  
            .post(&format!("{}/v1/Nexuts/get_best_instance", self.nexuts_url))  
            .json(&request)  
            .send()  
            .await?;  
  
        if !response.status().is_success() {  
            return Err(format!("Nexuts API returned status: {}", response.status()).into());  
        }  
  
        let nexuts_response: NexutsResponse = response.json().await?;  
          
        // 检查返回的worker_ids数组是否为空  
        if nexuts_response.worker_ids.is_empty() {  
            return Err("Nexuts API returned empty worker_ids array".into());  
        }  
          
        // 取第一个worker ID  
        // TODO:如果第一个获取失败再获取第二个、第三个。
        let first_worker_id = &nexuts_response.worker_ids[0];  
        debug!("Nexuts API returned {} worker_ids, selected first: {} (confidence: {})",   
               nexuts_response.worker_ids.len(), first_worker_id, nexuts_response.confidence);  
          
        Ok(first_worker_id.clone())  
    }  
  
    /// 将请求文本转换为 token IDs  
    fn tokenize_request(&self, request_text: &str) -> Result<Vec<u32>, Box<dyn std::error::Error + Send + Sync>> { 
        let encoding = self.tokenizer.encode(request_text)  
            .map_err(|e| format!("Tokenization failed: {}", e))?;  
          
        Ok(encoding.token_ids().to_vec())  
    }  
}  

impl std::fmt::Debug for TokenCacheAwarePolicy {  
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {  
        f.debug_struct("TokenCacheAwarePolicy")  
            .field("config", &self.config)  
            .field("tokenizer", &"Arc<dyn Tokenizer>")  
            .field("nexuts_url", &self.nexuts_url)  
            .finish()  
    }  
}

impl LoadBalancingPolicy for TokenCacheAwarePolicy {  
    fn select_worker(  
        &self,  
        workers: &[Arc<dyn Worker>],  
        request_text: Option<&str>,
        token_ids: Option<&[u32]>,
    ) -> Option<usize> {  
        let healthy_indices = get_healthy_worker_indices(workers);  
  
        if healthy_indices.is_empty() {  
            return None;  
        }  
  
        // 获取当前负载统计  
        let loads: Vec<usize> = workers.iter().map(|w| w.load()).collect();  
        let max_load = *loads.iter().max().unwrap_or(&0);  
        let min_load = *loads.iter().min().unwrap_or(&0);  
  
        // 检查负载是否不平衡  
        let is_imbalanced = max_load.saturating_sub(min_load) > self.config.balance_abs_threshold  
            && (max_load as f32) > (min_load as f32 * self.config.balance_rel_threshold);  
  
        if is_imbalanced {  
            // 负载不平衡时使用最短队列策略  
            debug!(  
                "Load balancing triggered | max: {} | min: {}",  
                max_load, min_load  
            );  
  
            RouterMetrics::record_load_balancing_event();  
            RouterMetrics::set_load_range(max_load, min_load);  
  
            let min_load_idx = healthy_indices  
                .iter()  
                .min_by_key(|&&idx| workers[idx].load())  
                .copied()?;  
  
            workers[min_load_idx].increment_processed();  
            RouterMetrics::record_processed_request(workers[min_load_idx].url());  
            RouterMetrics::record_policy_decision(self.name(), workers[min_load_idx].url());  
  
            return Some(min_load_idx);  
        }  
  
        // 负载平衡时使用外部缓存匹配
        // 优先使用传入的 token_ids (gRPC PD 模式)  
        let token_ids_vec = if let Some(ids) = token_ids {  
            ids.to_vec()  
        } else {  
            // 回退到 tokenization (非 gRPC PD 模式或测试场景)
            let text = request_text.unwrap_or("");  
            
            if text.is_empty() {  
                // 空请求,返回随机健康 worker  
                let mut rng = rand::rng();  
                let random_idx = rng.random_range(0..healthy_indices.len());  
                return Some(healthy_indices[random_idx]);  
            }  
    
            // 将请求文本转换为 token IDs  
            match self.tokenize_request(text) {  
                Ok(ids) => ids,  
                Err(e) => {  
                    warn!("Failed to tokenize request: {}, falling back to random selection", e);  
                    let mut rng = rand::rng();  
                    let random_idx = rng.random_range(0..healthy_indices.len());  
                    return Some(healthy_indices[random_idx]);
                }
            }  
        };  
  
        // 调用 Nexuts API  
        let rt = tokio::runtime::Handle::current();  
        let worker_id = match rt.block_on(self.call_nexuts_api(token_ids_vec)) {  
            Ok(id) => id,  
            Err(e) => {  
                warn!("Failed to call Nexuts API: {}, falling back to random selection", e);  
                let mut rng = rand::rng();  
                let random_idx = rng.random_range(0..healthy_indices.len());  
                return Some(healthy_indices[random_idx]);  
            }  
        };  
  
        // 在 workers 列表中查找对应的 worker  
        // TODO:解析worker_id为ip:port,在workers中查找对应worker
        if let Some(selected_idx) = workers.iter().position(|w| w.url() == worker_id) {  
            // 确保选中的 worker 是健康的  
            if workers[selected_idx].is_healthy() {  
                workers[selected_idx].increment_processed();  
                RouterMetrics::record_processed_request(&worker_id);  
                RouterMetrics::record_policy_decision(self.name(), &worker_id);  
                RouterMetrics::record_cache_hit();  
                return Some(selected_idx);  
            }  
        }  
  
        // 如果找不到对应的 worker 或 worker 不健康,回退到随机选择  
        debug!("Worker {} not found or unhealthy, falling back to random selection", worker_id);  
        let mut rng = rand::rng();  
        let random_idx = rng.random_range(0..healthy_indices.len());  
        Some(healthy_indices[random_idx])  
    }  
  
    fn name(&self) -> &'static str {  
        "token_cache_aware"  
    }  
  
    fn needs_request_text(&self) -> bool {  
        true // Token cache-aware policy 需要请求文本或 token_ids  
    }  
  
    fn on_request_complete(&self, worker_url: &str, success: bool) {  
        if !success {  
            debug!(  
                "Request to {} completed with success={}",  
                worker_url,  
                success  
            );  
        }  
    }  
  
    fn as_any(&self) -> &dyn std::any::Any {  
        self  
    }  
  
    fn select_worker_pair(  
        &self,  
        prefill_workers: &[Arc<dyn Worker>],  
        decode_workers: &[Arc<dyn Worker>],  
        request_text: Option<&str>,
        token_ids: Option<&[u32]>,
    ) -> Option<(usize, usize)> {  
        // Prefill 使用 token cache-aware 逻辑,传递 token_ids 
        let prefill_idx = self.select_worker(prefill_workers, request_text, token_ids)?;  
  
        // Decode 使用最短负载逻辑  
        let healthy_decode = get_healthy_worker_indices(decode_workers);  
        if healthy_decode.is_empty() {  
            return None;  
        }  
  
        let decode_idx = healthy_decode  
            .iter()  
            .min_by_key(|&&idx| decode_workers[idx].load())  
            .copied()?;  
  
        Some((prefill_idx, decode_idx))  
    }  
}