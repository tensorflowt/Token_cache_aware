//! Factory for creating load balancing policies  
  
use std::sync::Arc;  
  
use super::{  
    CacheAwareConfig, CacheAwarePolicy, LoadBalancingPolicy, PowerOfTwoPolicy, RandomPolicy,  
    RoundRobinPolicy, TokenCacheAwarePolicy,  
};  
use crate::config::PolicyConfig;  
use crate::tokenizer::traits::Tokenizer;  
  
/// Factory for creating policy instances  
pub struct PolicyFactory;  
  
impl PolicyFactory {  
    /// Create a policy from configuration  
    pub fn create_from_config(config: &PolicyConfig) -> Arc<dyn LoadBalancingPolicy> {  
        match config {  
            PolicyConfig::Random => Arc::new(RandomPolicy::new()),  
            PolicyConfig::RoundRobin => Arc::new(RoundRobinPolicy::new()),  
            PolicyConfig::PowerOfTwo { .. } => Arc::new(PowerOfTwoPolicy::new()),  
            PolicyConfig::CacheAware {  
                cache_threshold,  
                balance_abs_threshold,  
                balance_rel_threshold,  
                eviction_interval_secs,  
                max_tree_size,  
                enable_cache_sync,  
                sync_interval_secs,  
            } => {  
                let config = CacheAwareConfig {  
                    cache_threshold: *cache_threshold,  
                    balance_abs_threshold: *balance_abs_threshold,  
                    balance_rel_threshold: *balance_rel_threshold,  
                    eviction_interval_secs: *eviction_interval_secs,  
                    max_tree_size: *max_tree_size,  
                    enable_cache_sync: *enable_cache_sync,  
                    sync_interval_secs: *sync_interval_secs,  
                };  
                Arc::new(CacheAwarePolicy::with_config(config))  
            },
            PolicyConfig::TokenCacheAware {  
                cache_threshold,  
                balance_abs_threshold,  
                balance_rel_threshold,  
                eviction_interval_secs,  
                max_tree_size,  
                nexuts_url,  
            } => {  
                // 注意：这个方法无法创建 TokenCacheAwarePolicy，因为需要 tokenizer  
                // 实际使用时应该调用 create_from_config_with_tokenizer  
                panic!("TokenCacheAware policy requires tokenizer. Use create_from_config_with_tokenizer instead.");  
            }  
        }  
    }  
  
    /// Create a policy from configuration with tokenizer support  
    pub fn create_from_config_with_tokenizer(  
        config: &PolicyConfig,  
        tokenizer: Option<Arc<dyn Tokenizer>>,  
    ) -> Arc<dyn LoadBalancingPolicy> {  
        match config {  
            PolicyConfig::Random => Arc::new(RandomPolicy::new()),  
            PolicyConfig::RoundRobin => Arc::new(RoundRobinPolicy::new()),  
            PolicyConfig::PowerOfTwo { .. } => Arc::new(PowerOfTwoPolicy::new()),  
            PolicyConfig::CacheAware {  
                cache_threshold,  
                balance_abs_threshold,  
                balance_rel_threshold,  
                eviction_interval_secs,  
                max_tree_size,  
                enable_cache_sync,  
                sync_interval_secs,  
            } => {  
                let config = CacheAwareConfig {  
                    cache_threshold: *cache_threshold,  
                    balance_abs_threshold: *balance_abs_threshold,  
                    balance_rel_threshold: *balance_rel_threshold,  
                    eviction_interval_secs: *eviction_interval_secs,  
                    max_tree_size: *max_tree_size,  
                    enable_cache_sync: *enable_cache_sync,  
                    sync_interval_secs: *sync_interval_secs,  
                };  
                Arc::new(CacheAwarePolicy::with_config(config))  
            }  
            PolicyConfig::TokenCacheAware {  
                cache_threshold,  
                balance_abs_threshold,  
                balance_rel_threshold,  
                eviction_interval_secs,  
                max_tree_size,  
                nexuts_url,  
            } => {  
                let config = CacheAwareConfig {  
                    cache_threshold: *cache_threshold,  
                    balance_abs_threshold: *balance_abs_threshold,  
                    balance_rel_threshold: *balance_rel_threshold,  
                    eviction_interval_secs: *eviction_interval_secs,  
                    max_tree_size: *max_tree_size,  
                    enable_cache_sync: false,  
                    sync_interval_secs: 0,  
                };  
                let tokenizer = tokenizer.expect("TokenCacheAware policy requires tokenizer");  
                Arc::new(TokenCacheAwarePolicy::with_config_and_url(  
                    config,  
                    nexuts_url.clone(),  
                    tokenizer,  
                ))  
            }  
        }  
    }  
  
    /// Create a policy by name (for dynamic loading)  
    pub fn create_by_name(name: &str) -> Option<Arc<dyn LoadBalancingPolicy>> {  
        match name.to_lowercase().as_str() {  
            "random" => Some(Arc::new(RandomPolicy::new())),  
            "round_robin" | "roundrobin" => Some(Arc::new(RoundRobinPolicy::new())),  
            "power_of_two" | "poweroftwo" => Some(Arc::new(PowerOfTwoPolicy::new())),  
            "cache_aware" | "cacheaware" => Some(Arc::new(CacheAwarePolicy::new())),  
            "token_cache_aware" | "tokencacheaware" => {  
                // 无法通过名称创建，因为需要 tokenizer  
                warn!("Cannot create TokenCacheAware policy by name without tokenizer. Use create_from_config_with_tokenizer instead.");  
                None  
            }  
            _ => None,  
        }  
    }  
  
    /// Create a policy by name with tokenizer support  
    pub fn create_by_name_with_tokenizer(  
        name: &str,  
        tokenizer: Arc<dyn Tokenizer>,  
    ) -> Option<Arc<dyn LoadBalancingPolicy>> {  
        match name.to_lowercase().as_str() {  
            "random" => Some(Arc::new(RandomPolicy::new())),  
            "round_robin" | "roundrobin" => Some(Arc::new(RoundRobinPolicy::new())),  
            "power_of_two" | "poweroftwo" => Some(Arc::new(PowerOfTwoPolicy::new())),  
            "cache_aware" | "cacheaware" => Some(Arc::new(CacheAwarePolicy::new())),  
            "token_cache_aware" | "tokencacheaware" => {  
                Some(Arc::new(TokenCacheAwarePolicy::new(tokenizer)))  
            }  
            _ => None,  
        }  
    }  
}  
  
#[cfg(test)]  
mod tests {  
    use super::*;  
    use crate::tokenizer::mock::MockTokenizer;  
  
    #[test]  
    fn test_create_from_config() {  
        let policy = PolicyFactory::create_from_config(&PolicyConfig::Random);  
        assert_eq!(policy.name(), "random");  
  
        let policy = PolicyFactory::create_from_config(&PolicyConfig::RoundRobin);  
        assert_eq!(policy.name(), "round_robin");  
  
        let policy = PolicyFactory::create_from_config(&PolicyConfig::PowerOfTwo {  
            load_check_interval_secs: 60,  
        });  
        assert_eq!(policy.name(), "power_of_two");  
  
        let policy = PolicyFactory::create_from_config(&PolicyConfig::CacheAware {  
            cache_threshold: 0.7,  
            balance_abs_threshold: 10,  
            balance_rel_threshold: 1.5,  
            eviction_interval_secs: 30,  
            max_tree_size: 1000,  
            enable_cache_sync: false,  
            sync_interval_secs: 0,  
        });  
        assert_eq!(policy.name(), "cache_aware");  
    }  
  
    #[test]  
    #[should_panic(expected = "TokenCacheAware policy requires tokenizer")]  
    fn test_create_token_cache_aware_without_tokenizer_should_panic() {  
        PolicyFactory::create_from_config(&PolicyConfig::TokenCacheAware {  
            cache_threshold: 0.7,  
            balance_abs_threshold: 10,  
            balance_rel_threshold: 1.5,  
            eviction_interval_secs: 30,  
            max_tree_size: 1000,  
            nexuts_url: "http://localhost:8080".to_string(),  
        });  
    }  
  
    #[test]  
    fn test_create_from_config_with_tokenizer() {  
        let tokenizer = Arc::new(MockTokenizer::new());  
          
        let policy = PolicyFactory::create_from_config_with_tokenizer(  
            &PolicyConfig::TokenCacheAware {  
                cache_threshold: 0.7,  
                balance_abs_threshold: 10,  
                balance_rel_threshold: 1.5,  
                eviction_interval_secs: 30,  
                max_tree_size: 1000,  
                nexuts_url: "http://localhost:8080".to_string(),  
            },  
            Some(tokenizer),  
        );  
        assert_eq!(policy.name(), "token_cache_aware");  
    }  
  
    #[test]  
    fn test_create_by_name() {  
        assert!(PolicyFactory::create_by_name("random").is_some());  
        assert!(PolicyFactory::create_by_name("RANDOM").is_some());  
        assert!(PolicyFactory::create_by_name("round_robin").is_some());  
        assert!(PolicyFactory::create_by_name("RoundRobin").is_some());  
        assert!(PolicyFactory::create_by_name("power_of_two").is_some());  
        assert!(PolicyFactory::create_by_name("PowerOfTwo").is_some());  
        assert!(PolicyFactory::create_by_name("cache_aware").is_some());  
        assert!(PolicyFactory::create_by_name("CacheAware").is_some());  
          
        // TokenCacheAware 无法通过 create_by_name 创建  
        assert!(PolicyFactory::create_by_name("token_cache_aware").is_none());  
    }  
  
    #[test]  
    fn test_create_by_name_with_tokenizer() {  
        let tokenizer = Arc::new(MockTokenizer::new());  
          
        assert!(PolicyFactory::create_by_name_with_tokenizer("random", tokenizer.clone()).is_some());  
        assert!(PolicyFactory::create_by_name_with_tokenizer("token_cache_aware", tokenizer).is_some());  
    }  
}