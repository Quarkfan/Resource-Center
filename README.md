# Resource Center

Content-addressed resource, artifact and diagnostics service for QuarkfanTools 3.0. Local volumes implement the first deployment while preserving an S3-compatible storage boundary.

Local filesystem storage, diagnostic ZIP generation and FFmpeg processing are explicit Resource Provider extensions. Put, diagnostics and media admission resolve the corresponding extension before side effects.
Extension state, descriptor generation, probes and events are stored transactionally in the Resource PostgreSQL schema and survive service replacement.
