# What is Pulse?



<figure><img src=".gitbook/assets/Pulsecron_logo.png" alt=""><figcaption></figcaption></figure>

## **Definition and Context**:

* Pulse is an open-source job scheduling tool in the Node.js ecosystem, serving as a fork of the discontinued Agenda project.
* Utilizes PostgreSQL for managing job data, which enhances its ability to perform at scale.

## Unique Features in Pulse

* **PostgreSQL Storage**: Jobs live in a single PostgreSQL table that Pulse locks with `FOR UPDATE SKIP LOCKED`, so many workers can share one queue. You own the `pg` Pool; Pulse never ends it.
* **Resume Incomplete Tasks After System Restart**: When the system restarts, Pulse resumes incomplete tasks that were in progress or queued for execution, providing seamless continuation without manual intervention.
* **Retry Failed Tasks**: Pulse offers retry mechanisms using exponential and fixed backoff strategies with configurable attempts, ensuring efficient retries of failed tasks without overwhelming the system.
* **Continuous Maintenance**: As an open-source project actively used in a production service, Pulse is consistently maintained and improved, providing users with reliable updates and support.
* **Extensive Documentation**: Provides detailed guides and examples for a quick and easy start.

## **Related Projects**:

* [**pulsecron**](https://www.pulsecron.com): Provides an event-driven task scheduling management infrastructure.
* [**nestjs-pulse**](https://github.com/pulsecron/nestjs-pulse): An official NestJS module specifically tailored for integrating Pulse into NestJS applications.

## **Usage and Application**:

* Ideal for developers needing a reliable, scalable, and modern job scheduling solution.
* Applicable in scenarios ranging from simple task automation to complex job orchestration in distributed systems.
