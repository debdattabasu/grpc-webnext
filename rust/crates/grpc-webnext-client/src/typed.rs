//! Typed helpers over `prost::Message`.
//!
//! Thin by design: encoding is `prost`'s job and framing is `codec`'s, so this is
//! only the two conversions plus turning a decode failure into a gRPC status rather
//! than a panic — a server that sends something unparseable is INTERNAL, not a
//! crash in the tab.

use futures::stream::{Stream, StreamExt};
use prost::Message;

use crate::client::{CallOptions, Client, UnaryResponse};
use crate::metadata::Metadata;
use crate::status::{Code, Status};

/// A decoded response plus both metadata blocks.
#[derive(Debug, Clone)]
pub struct TypedResponse<M> {
    pub message: M,
    pub headers: Metadata,
    pub trailers: Metadata,
}

impl<M> TypedResponse<M> {
    /// Discard the metadata when only the message matters.
    pub fn into_inner(self) -> M {
        self.message
    }
}

/// Typed calls over a [`Client`].
pub trait TypedClient {
    /// Unary with prost encode/decode.
    fn unary_typed<Req, Res>(
        &self,
        path: &str,
        request: Req,
        options: CallOptions,
    ) -> impl std::future::Future<Output = Result<TypedResponse<Res>, Status>>
    where
        Req: Message,
        Res: Message + Default;

    /// Client streaming with prost encode/decode.
    fn client_streaming_typed<Req, Res, S>(
        &self,
        path: &str,
        requests: S,
        options: CallOptions,
    ) -> impl std::future::Future<Output = Result<TypedResponse<Res>, Status>>
    where
        Req: Message + 'static,
        Res: Message + Default,
        S: Stream<Item = Req> + 'static;
}

impl TypedClient for Client {
    async fn unary_typed<Req, Res>(
        &self,
        path: &str,
        request: Req,
        options: CallOptions,
    ) -> Result<TypedResponse<Res>, Status>
    where
        Req: Message,
        Res: Message + Default,
    {
        let response = self.unary(path, request.encode_to_vec(), options).await?;
        decode(response)
    }

    async fn client_streaming_typed<Req, Res, S>(
        &self,
        path: &str,
        requests: S,
        options: CallOptions,
    ) -> Result<TypedResponse<Res>, Status>
    where
        Req: Message + 'static,
        Res: Message + Default,
        S: Stream<Item = Req> + 'static,
    {
        let response = self
            .client_streaming(path, requests.map(|m| m.encode_to_vec()), options)
            .await?;
        decode(response)
    }
}

fn decode<Res: Message + Default>(response: UnaryResponse) -> Result<TypedResponse<Res>, Status> {
    let message = Res::decode(response.message.as_slice()).map_err(|e| {
        Status::new(Code::Internal, format!("failed to decode response message: {e}"))
    })?;
    Ok(TypedResponse { message, headers: response.headers, trailers: response.trailers })
}
