#########
Telemetry
#########

The MSC provides telemetry through the `OpenTelemetry Python API and SDK <https://github.com/open-telemetry/opentelemetry-python>`_.

Only the OpenTelemetry Python API is included by default. The OpenTelemetry Python SDK is included with the ``observability-otel`` extra.

Telemetry can be configured with the ``opentelemetry`` dictionary in the MSC configuration. See :doc:`/references/configuration` for all configuration options.

.. code-block:: yaml
   :caption: Example MSC configuration.

   profiles:
     data:
       # ...
   opentelemetry:
     metrics:
       attributes:
         - type: static
           options:
             attributes:
               organization: NVIDIA
               cluster: DGX SuperPOD 1
         - type: host
           options:
             attributes:
               node: name
         - type: process
           options:
             attributes:
               process: pid
       reader:
         options:
           # ≤ 100 Hz collect frequency.
           collect_interval_millis: 10
           collect_timeout_millis: 100
           # ≤ 1 Hz export frequency.
           export_interval_millis: 1000
           export_timeout_millis: 500
       exporter:
         type: otlp
         options:
           # OpenTelemetry Collector default local HTTP endpoint.
           endpoint: http://localhost:4318/v1/traces
     traces:
       exporter:
         type: otlp
         options:
           # OpenTelemetry Collector default local HTTP endpoint.
           endpoint: http://localhost:4318/v1/traces

.. code-block:: python
   :caption: Example usage with automatic telemetry initialization.

   import multistorageclient

   # Directly create a storage client for a profile and open an object/file.
   client = multistorageclient.StorageClient(
       config=multistorageclient.StorageClientConfig.from_file(profile="data")
   )
   client.open("file.txt")

   # Use an MSC shortcut to create a storage client for a profile and open an object/file.
   multistorageclient.open("msc://data/file.txt")

This will automatically create telemetry provider instances which will spin up dedicated telemetry processes and local network ports (for IPC) as needed.

If the default telemetry provider creation doesn't behave as desired, you can manually create a telemetry provider to use with storage client creation flows.

.. code-block:: python
   :caption: Example usage with manual telemetry initialization.

   import multistorageclient
   import multistorageclient.telemetry

   # Create a telemetry provider instance.
   #
   # Based on the mode, this may create a Python multiprocessing manager server
   # listening on 127.0.0.1:{dynamic port based on the process ID} to make the
   # OpenTelemetry Python SDK work correctly with Python multiprocessing.
   #
   # The telemetry mode and address can be provided as function parameters.
   # See the API reference for more details.
   telemetry = multistorageclient.telemetry.init()

   # Directly create a storage client with the telemetry provider instance and open an object/file.
   client = multistorageclient.StorageClient(
       config=multistorageclient.StorageClientConfig.from_file(
           profile="data",
           telemetry=telemetry
       )
   )
   client.open("file.txt")

   # Set the telemetry provider instance to use when MSC shortcuts create storage clients.
   multistorageclient.set_telemetry(telemetry=telemetry)

   # Use an MSC shortcut to create a storage client for a profile and open an object/file.
   multistorageclient.open("msc://data/file.txt")

*******
Metrics
*******

MSC prefers publishing raw samples when possible to support arbitrary post-hoc aggregations.

This is done through high frequency gauges, with sums being used for accurate global aggregates.

Concepts
========

Theory
------

.. glossary::

   `sample <https://en.wikipedia.org/wiki/Sampling_(statistics)>`_
      Individual metric data point.

   `distribution <https://en.wikipedia.org/wiki/Probability_distribution>`_
      Collection of samples.

   true distribution
      A distribution with all samples (e.g. true distribution of fair 6-sided dice rolls).

      This may have infinite samples.

   `empirical distribution <https://en.wikipedia.org/wiki/Empirical_distribution_function>`_
      A distribution with a subset of samples (e.g. empirical distribution of 1000 fair 6-sided dice rolls).

   `aggregate <https://en.wikipedia.org/wiki/Aggregate_function>`_
      Compress a distribution into a summary statistic (e.g. minimum, maximum, sum, average, percentile).

   `decomposable aggregate <https://en.wikipedia.org/wiki/Aggregate_function#Decomposable_aggregate_functions>`_
      An aggregate which can be recursively applied.

      For example, the maximum is a decomposable aggregate because the global maximum can be found by taking the maximum of the local maxima of sample subsets.

      On the other hand, the average is not a decomposable aggregate because the global average cannot be found by taking the average of the local averages of sample subsets.

   `sampling rate <https://en.wikipedia.org/wiki/Sampling_(signal_processing)#Sampling_rate>`_
      For a signal over time (e.g. metric data points over time), this is how often a sample is collected.

OpenTelemetry
-------------

OpenTelemetry provides several metric points. Of note are:

.. glossary::

   `gauge <https://opentelemetry.io/docs/specs/otel/metrics/data-model#gauge>`_
      Captures a distribution.

      If the sampling rate is high enough, this captures the true distribution.

      If the sampling rate is not high enough, this captures the empirical distribution. This preserves local (i.e. per-sample) information at the expense of global (i.e. aggregate) information.

   `sum <https://opentelemetry.io/docs/specs/otel/metrics/data-model#sums>`_
      Captures sums, a decomposable aggregate.

      This preserves global (i.e. aggregate) information at the expense of local (i.e. per-sample) information.

   `histogram <https://opentelemetry.io/docs/specs/otel/metrics/data-model#histogram>`_
      Captures a distribution by bucketing samples by value.

      Not used by the MSC since buckets must be pre-defined, requiring the distribution to be known ahead of time.

Emitted Metrics
===============

Storage Provider
----------------

.. glossary::

   ``multistorageclient.latency``
      The time it took for an operation to complete.

      * Operations:

        * All

      * Metric data point:

        * Gauge

      * Unit:

        * Seconds

      * Attributes:

        * ``multistorageclient.provider`` (e.g. ``s3``)
        * ``multistorageclient.operation`` (e.g. ``read``)
        * ``multistorageclient.status`` (e.g. ``success``, ``error.{Python error class name}``)

      * Timestamp:

        * Operation End

   ``multistorageclient.data_size``
      The data (object/file) size for an operation.

      * Operations:

        * Successful Read, Write, Copy

      * Metric data point:

        * Gauge

      * Unit:

        * Bytes

      * Attributes:

        * ``multistorageclient.provider`` (e.g. ``s3``)
        * ``multistorageclient.operation`` (e.g. ``read``)
        * ``multistorageclient.status`` (e.g. ``success``, ``error.{Python error class name}``)

      * Timestamp:

        * Operation End

   ``multistorageclient.data_rate``
      The data size divided by the latency for an operation. Equivalent to an operation's average data rate.

      * Operations:

        * Successful Read, Write, Copy

      * Metric data point:

        * Gauge

      * Unit:

        * Bytes/Second

      * Attributes:

        * ``multistorageclient.provider`` (e.g. ``s3``)
        * ``multistorageclient.operation`` (e.g. ``read``)
        * ``multistorageclient.status`` (e.g. ``success``, ``error.{Python error class name}``)

      * Timestamp:

        * Operation End

   ``multistorageclient.request.sum``
      The sum of operation starts.

      * Operations:

        * All

      * Metric data point:

        * Sum

      * Unit:

        * Requests

      * Attributes:

        * ``multistorageclient.provider`` (e.g. ``s3``)
        * ``multistorageclient.operation`` (e.g. ``read``)

      * Timestamp:

        * Operation Start

   ``multistorageclient.response.sum``
      The sum of operation ends.

      * Operations:

        * All

      * Metric data point:

        * Sum

      * Unit:

        * Responses

      * Attributes:

        * ``multistorageclient.provider`` (e.g. ``s3``)
        * ``multistorageclient.operation`` (e.g. ``read``)
        * ``multistorageclient.status`` (e.g. ``success``, ``error.{Python error class name}``)

      * Timestamp:

        * Operation End

   ``multistorageclient.data_size.sum``
      The data (object/file) size for all operations.

      * Operations:

        * Successful Read, Write, Copy

      * Metric data point:

        * Sum

      * Unit:

        * Bytes

      * Attributes:

        * ``multistorageclient.provider`` (e.g. ``s3``)
        * ``multistorageclient.operation`` (e.g. ``read``)
        * ``multistorageclient.status`` (e.g. ``success``, ``error.{Python error class name}``)

      * Timestamp:

        * Operation End

******
Traces
******

MSC publishes spans using a tail sampler which publishes errors and high-latency traces. The span pipeline currently isn't configurable except the exporter.
