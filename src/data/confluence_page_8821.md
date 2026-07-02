# Secret Rotation Protocol
**Page ID:** 8821
**Author:** John (john.dba@ecommerce.internal)
**Last Modified:** 2025-11-12

## Overview
This document outlines the standard operating procedure (SOP) for rotating API keys, database passwords, and webhook secrets across our AWS environments.

## Procedure
1. **Generate New Secret:** Use the provider's dashboard (e.g., Stripe, SendGrid) to generate a new key.
2. **Update AWS Secrets Manager:** Log into the AWS Console, locate the relevant secret (e.g., `prod/stripe/webhook`), and input the new value.
3. **Restart Services:** Secrets are cached in memory on pod startup. You MUST perform a rolling restart of the backend Kubernetes deployments for the new secrets to take effect.

## Troubleshooting
If webhooks begin failing with 401 Unauthorized errors after a rotation, it usually means the Kubernetes pods were not restarted. If this occurs, open a bug ticket in Jira under the `SHOP` project and alert the `#backend-dev` Slack channel immediately.