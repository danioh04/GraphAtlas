# Mobile Push Notification Architecture
**Page ID:** 9934
**Author:** Emily (emily.mobile@ecommerce.internal)
**Last Modified:** 2026-02-04

## System Flow
Our push notification infrastructure relies on AWS SNS to broadcast messages to both iOS (APNs) and Android (FCM) devices.

1. The `OrderService` publishes an event to an SNS Topic.
2. SNS routes the payload to the respective platform endpoints.
3. The mobile clients handle the payload and render the system notification.

## Maintenance
Apple requires APNs certificates to be renewed annually. The backend team is responsible for managing these certificates in AWS. Failure to renew these will result in an immediate drop in iOS delivery rates. Track renewal tasks using standard Jira tasks, ensuring they are linked to any resulting incident tickets.