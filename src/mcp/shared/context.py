from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic

from typing_extensions import TypeVar

from mcp.client.transport_session import ClientTransportSession
from mcp.server.transport_session import ServerTransportSession
from mcp.types import RequestId, RequestParams

SessionT = TypeVar(
    "SessionT", bound=ClientTransportSession | ServerTransportSession
)
LifespanContextT = TypeVar("LifespanContextT")
RequestT = TypeVar("RequestT", default=Any)


@dataclass
class RequestContext(Generic[SessionT, LifespanContextT, RequestT]):
    request_id: RequestId
    meta: RequestParams.Meta | None
    session: SessionT
    lifespan_context: LifespanContextT
    request: RequestT | None = None
