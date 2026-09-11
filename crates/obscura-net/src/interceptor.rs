use std::collections::HashMap;

use crate::client::{RequestInfo, Response};

pub enum InterceptAction {
    Continue,
    Block,
    Fulfill(Response),
    ModifyHeaders(HashMap<String, String>),
}

#[async_trait::async_trait]
pub trait RequestInterceptor {
    /// Time reserved for an external request controller before a resource
    /// warmup budget may cancel the request. Ordinary interceptors retain the
    /// caller's existing budget.
    fn minimum_wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    async fn intercept(&self, request: &RequestInfo) -> InterceptAction;
}
