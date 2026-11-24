# Security and safety scope

This is deliberately not a payment service. It has no public endpoint, secrets, real-provider integration, or real payment details.

Do not add real customer or card data, live credentials, or internal infrastructure identifiers. The baseline contains intentional flaws for comparison and MUST NOT be adapted into a real payment pipeline. The synthetic event handler assumes prior authentication and must not be used as a webhook receiver.

The privacy check searches for common identifiable patterns; it is not a substitute for review of every file, Git history, screenshots, artifacts, or build logs.
