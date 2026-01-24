This is the original blog post from Google that introduces the need for gRPC with MCP.

1/24/26, 7:18 AM gRPC as a custom transport for MCP | Google Cloud Blog 

AI agents are moving from test environments to the core of enterprise operations, where they must interact reliably with external tools and systems to execute complex, multi-step goals. The Model Context Protocol (MCP) is the standard that makes this agent to tool communication possible. In fact, just last month we announced the release of fully managed, remote MCP servers. Developers can now simply point their AI agents or standard MCP clients like Gemini CLI to a globally-consistent and enterprise-ready endpoint for Google and Google Cloud services. 

MCP uses JSON-RPC as its standard transport. This brings many benefits as it combines an action-oriented approach with natural language payloads that can be directly relayed by agents in their communication with foundational models. Yet many organizations rely on gRPC, a high-performance, open source implementation of the remote procedure call (RPC) model. Enterprises that have adopted the gRPC framework must adapt their tooling to be compatible with the JSON-RPC transport used by MCP. Today, these enterprises need to deploy transcoding gateways to translate between JSON-RPC MCP requests and their existing gRPC-based services.  

An interesting alternative to MCP transcoding is to use gRPC as the custom transport for MCP. Many gRPC users are actively experimenting with this option by implementing their own custom MCP servers. At Google Cloud, we use gRPC extensively to enable services 

and offer APIs at a global scale, and we’re committed to sharing the technology and expertise that has resulted from this pervasive use of gRPC. Specifically, we’re committed to supporting gRPC practitioners in their journey to adopt MCP in production, and we’re actively working with the MCP community to explore mechanisms to support gRPC as a transport for MCP. The MCP core maintainers have arrived at an agreement to support pluggable transports in the MCP SDK, and in the near future, Google Cloud will contribute and distribute a gRPC transport package to be plugged into the MCP SDKs. A community backed transport package will enable gRPC practitioners to deploy MCP with gRPC in a consistent and interoperable manner. 

The  use of gRPC as a transport avoids the need for transcoding and helps maintain operational consistency for environments that are actively using gRPC. In the rest of this post, we explore the benefits of using gRPC as a  transport for MCP and how Google Cloud is supporting this journey. 

**The choice of RPC transport** 

For organizations already using gRPC for their services, gRPC support allows them to continue to use their existing tooling to access services via MCP without altering the services or implementing transcoding proxies. These organizations are on a journey to keep the benefits of gRPC as MCP becomes the mechanism for agents to access services. 

https://cloud.google.com/blog/products/networking/grpc-as-a-native-transport-for-mcp 1/4  
1/24/26, 7:18 AM gRPC as a custom transport for MCP | Google Cloud Blog *“Because gRPC is our standard protocol in the backend, we have invested in experimental support for MCP over gRPC internally. And we already see the benefits: ease of use and familiarity for our developers, and reducing the work needed to build MCP servers by using the structure and statically typed APIs.”* \-  **Stefan Särne, Senior Staff Engineer and Tech Lead for Developer Experience, Spotify ** 

**Benefits of gRPC** 

Using gRPC as a transport aligns MCP with the best practices of modern gRPC-based distributed systems, improving performance, security, operations, and developer productivity. 

**Performance and efficiency** 

The performance advantages of gRPC provide a big boost in efficiency, thanks to the following attributes: 

**Binary encoding (protocol buffers)**: gRPC uses protocol buffers (Protobufs) for binary encoding, shrinking message sizes by up to 10x compared to JSON. This means less bandwidth consumption and faster serialization/deserialization, which translates to lower latency for tool calls, reduced network costs, and a much smaller resource footprint. 

**Full duplex bidirectional streaming**: gRPC natively supports the client (the agent) and the server (the tool), sending continuous data streams to each other simultaneously over a single, persistent connection. This feature is a game-changer for agent-tool interaction, opening the door to truly interactive, real-time agentic workflows without requiring application-level connection synchronization.  

**Built-in flow control (backpressure)**: gRPC includes native flow control to prevent a fast-sending tool from overwhelming the agent. 

**Enterprise-grade security and authorization** 

gRPC treats security as a first-class citizen, with enterprise-grade features built directly into its core, including: 

**Mutual TLS (mTLS)**: Critical for Zero Trust architectures, mTLS authenticates both the client and the gRPC-powered server, preventing spoofing and helping to ensure only trusted services communicate. 

**Strong authentication**: gRPC offers native hooks for integrating with industry standard token-based authentication (JWT/OAuth), providing verifiable identity for every AI agent. 

https://cloud.google.com/blog/products/networking/grpc-as-a-native-transport-for-mcp 2/4  
1/24/26, 7:18 AM gRPC as a custom transport for MCP | Google Cloud Blog **Method-level authorization**: You can enforce authorization policies directly on specific RPC methods or MCP tools (e.g., an agent is authorized to ReadFile but not DeleteFile), helping to ensure strict adherence to the principle of least privilege and combating "excessive agency." 

**Operational maturity and developer productivity** 

gRPC provides a powerful, integrated solution that helps offload resiliency measures and improves developer productivity through extensibility and reusability. Some of its capabilities include: 

**Unified observability**: Native integration with distributed tracing (OpenTelemetry) and structured error codes provides a complete, auditable trail of every tool call. Developers can trace a single user prompt through every subsequent microservice interaction. 

**Robust resiliency**: Features like deadlines, timeouts, and automatic flow control prevent a single unresponsive tool from causing system-wide failures. These features allow a client to specify a policy for a tool call that the framework automatically cancels if exceeded, preventing a cascading failure. 

**Polyglot development**: gRPC generates code for 11+ languages, allowing developers to implement MCP Servers in the best language for the job while maintaining a consistent, strongly-typed contract. 

**Schema-based input validation**: Protobuf's strict typing mitigates injection attacks and simplifies the development task by rejecting malformed inputs at the serialization layer. 

**Error handling and metadata**: The framework provides a standardized set of error codes (e.g., UNAVAILABLE, PERMISSION\_DENIED) for reliable client handling, and clients can send and receive out-of-band information as key-value pairs in **metadata** (e.g., for tracing IDs) without cluttering the main request. 

**Get started** 

As a founding member of the Agentic AI Foundation and a core contributor to the MCP specification, Google Cloud, along with other members of the community, has championed the inclusion of pluggable transport interfaces in the MCP SDK. Participate and communicate your interest in having gRPC as a transport for MCP: 

Express your interest in enabling gRPC as an MCP transport. Contribute to the active pull request for pluggable transport interfaces for the Python MCP SDK.  

Join the community that is shaping the future of communications for AI and help advance the Model Context Protocol. Contributor Communication \- Model Context Protocol.  

https://cloud.google.com/blog/products/networking/grpc-as-a-native-transport-for-mcp 3/4  
1/24/26, 7:18 AM gRPC as a custom transport for MCP | Google Cloud Blog Contact us. We want to learn from your experience and support your journey. 

https://cloud.google.com/blog/products/networking/grpc-as-a-native-transport-for-mcp 4/4