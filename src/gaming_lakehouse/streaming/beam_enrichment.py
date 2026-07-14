"""Apache Beam pipeline: real-time enrichment + fraud/refund-abuse signals.

Runner is a config toggle:
    GCP   -> DataflowRunner (streaming engine, autoscaling, shuffle service)
    Azure -> FlinkRunner    (Beam portable runner on Flink/AKS)

Beam owns the *low-latency, per-event* path (sub-second windowed aggregates written to the
serving store). Spark Structured Streaming owns the *lakehouse* path (Bronze/Silver Delta).
Two engines, one contract: streaming/event_schemas.py.
"""

from __future__ import annotations

import argparse
import json
import logging
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)
from apache_beam.transforms import window


class ParseEvent(beam.DoFn):
    """Dead-letter anything unparseable instead of crashing the pipeline."""

    DLQ = "dlq"

    def process(self, element: bytes):
        try:
            payload: dict[str, Any] = json.loads(element.decode("utf-8"))
            if not payload.get("event_id") or not payload.get("player_id"):
                raise ValueError("missing mandatory keys")
            payload["net_revenue"] = round(
                payload["unit_price"] * payload["quantity"] * (1 - (payload.get("discount_pct") or 0)), 4
            )
            yield payload
        except Exception as exc:
            yield beam.pvalue.TaggedOutput(
                self.DLQ, json.dumps({"raw": element.decode("utf-8", "replace"), "error": str(exc)})
            )


class RevenueByPlatform(beam.PTransform):
    """1-minute sliding revenue per platform/channel — feeds the live ops dashboard."""

    def expand(self, pcoll):
        return (
            pcoll
            | "Window"
            >> beam.WindowInto(
                window.SlidingWindows(size=60, period=15),
                trigger=beam.transforms.trigger.AfterWatermark(
                    early=beam.transforms.trigger.AfterProcessingTime(5)
                ),
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
                allowed_lateness=300,
            )
            | "Key" >> beam.Map(lambda e: ((e["platform"], e["channel_code"]), e["net_revenue"]))
            | "Sum" >> beam.CombinePerKey(sum)
            | "Shape"
            >> beam.Map(lambda kv: {"platform": kv[0][0], "channel_code": kv[0][1], "net_revenue": kv[1]})
        )


def build_pipeline(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloud", default="gcp", choices=["gcp", "azure"])
    parser.add_argument("--input_subscription")  # GCP
    parser.add_argument("--bootstrap_servers")  # Azure
    parser.add_argument("--topic", default="gc-purchase-events")
    parser.add_argument("--output_table")  # BigQuery table or ADLS path
    parser.add_argument("--dlq_topic", default="gc-purchase-events-dlq")
    known, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).runner = "DataflowRunner" if known.cloud == "gcp" else "FlinkRunner"

    with beam.Pipeline(options=options) as pipeline:
        if known.cloud == "gcp":
            raw = pipeline | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=known.input_subscription, with_attributes=False
            )
        else:
            raw = (
                pipeline
                | "ReadEventHubs"
                >> beam.io.ReadFromKafka(
                    consumer_config={
                        "bootstrap.servers": known.bootstrap_servers,
                        "security.protocol": "SASL_SSL",
                        "sasl.mechanism": "PLAIN",
                        "auto.offset.reset": "latest",
                    },
                    topics=[known.topic],
                )
                | "DropKey" >> beam.Map(lambda kv: kv[1])
            )

        parsed = raw | "Parse" >> beam.ParDo(ParseEvent()).with_outputs(ParseEvent.DLQ, main="events")

        aggregates = parsed.events | "Revenue" >> RevenueByPlatform()

        if known.cloud == "gcp":
            _ = aggregates | "ToBigQuery" >> beam.io.WriteToBigQuery(
                known.output_table,
                schema="platform:STRING,channel_code:STRING,net_revenue:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,  # exactly-once, cheaper
                triggering_frequency=15,
                with_auto_sharding=True,
            )
            _ = parsed[ParseEvent.DLQ] | "DLQPubSub" >> beam.io.WriteToPubSub(
                topic=f"projects/{options.get_all_options().get('project')}/topics/{known.dlq_topic}"
            )
        else:
            _ = (
                aggregates
                | "Serialize" >> beam.Map(json.dumps)
                | "ToADLS" >> beam.io.WriteToText(known.output_table, file_name_suffix=".json")
            )
            _ = parsed[ParseEvent.DLQ] | "DLQADLS" >> beam.io.WriteToText(
                f"{known.output_table}_dlq", file_name_suffix=".json"
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    build_pipeline()
