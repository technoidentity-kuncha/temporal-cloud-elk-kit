#!/usr/bin/env python3
"""Generate Kibana 8.x saved objects (data view + Lens panels + 3 dashboards) as NDJSON.

    python3 elk/kibana/build_dashboards.py                 # writes elk/kibana/temporal-dashboards.ndjson
    python3 elk/kibana/build_dashboards.py --import http://localhost:5601

The NDJSON is the deliverable: import it in Kibana via Stack Management -> Saved Objects -> Import,
or with the --import flag (uses POST /api/saved_objects/_import?overwrite=true).

Field conventions (Metricbeat prometheus collector with use_types: true):
  SDK counters    prometheus.<name>_total.rate        delta since previous scrape  -> sum per interval
  SDK histograms  prometheus.<name>_seconds.histogram ES histogram field           -> percentile 95
  SDK gauges      prometheus.<name>.value
  Cloud metrics   prometheus.temporal_cloud_v1_*.value all gauges; *_count = per-second rate (1-min avg)
  Labels          prometheus.labels.*  (SDK: namespace, task_queue, sdk ...; Cloud: temporal_namespace, temporal_task_queue ...)
"""

import argparse
import json
import pathlib
import urllib.request
import uuid

NS = uuid.UUID("6f1c2d3e-0000-4000-8000-00000000c10d")
DATA_VIEW_ID = "temporal-metrics"
P = "prometheus."
L = "prometheus.labels."
SDK = "source_kind : temporal_sdk"
CLOUD = "source_kind : temporal_cloud"


def oid(name: str) -> str:
    return str(uuid.uuid5(NS, name))


# ---------------------------------------------------------------- Lens building blocks
def _metric_col(label, op, field, kql=None):
    col = {
        "label": label,
        "customLabel": True,
        "dataType": "number",
        "operationType": op,
        "sourceField": field,
        "isBucketed": False,
        "scale": "ratio",
        "params": {"emptyAsNull": True},
    }
    if op == "percentile":
        col["params"] = {"percentile": 95}
    if op == "last_value":
        col["params"] = {"sortField": "@timestamp", "showArrayValues": False}
        # Each Metricbeat doc carries one metric group, so "latest doc" must mean "latest doc with this field".
        kql = kql or f"{field} : *"
    if kql:
        col["filter"] = {"query": kql, "language": "kuery"}
    return col


def _date_col():
    return {
        "label": "@timestamp",
        "dataType": "date",
        "operationType": "date_histogram",
        "sourceField": "@timestamp",
        "isBucketed": True,
        "scale": "interval",
        "params": {"interval": "auto", "includeEmptyRows": True, "dropPartials": False},
    }


def _terms_col(field, order_col, size=10):
    return {
        "label": field.replace(L, ""),
        "customLabel": True,
        "dataType": "string",
        "operationType": "terms",
        "sourceField": field,
        "isBucketed": True,
        "scale": "ordinal",
        "params": {
            "size": size,
            "orderBy": {"type": "column", "columnId": order_col},
            "orderDirection": "desc",
            "otherBucket": False,
            "missingBucket": False,
            "parentFormat": {"id": "terms"},
        },
    }


def lens(title, description, metrics, *, split=None, kql="", chart="line", unit=None):
    """metrics: list of (label, op, field[, column-filter-kql]). One layer, x = time."""
    layer = "layer1"
    cols, order, accessors = {}, [], []
    if chart != "metric":
        cols["x"] = _date_col()
    for i, m in enumerate(metrics):
        cid = f"y{i}"
        cols[cid] = _metric_col(m[0], m[1], m[2], m[3] if len(m) > 3 else None)
        if unit == "s":
            cols[cid]["params"]["format"] = {"id": "number", "params": {"decimals": 3, "suffix": " s"}}
        accessors.append(cid)
    # Each Metricbeat document holds one metric group, so restrict the panel to documents that contain
    # one of its metrics. Otherwise breakdowns (top N terms) are chosen from unrelated documents.
    has_metric = "(" + " or ".join(f"{m[2]} : *" for m in metrics) + ")"
    kql = f"{kql} and {has_metric}" if kql else has_metric
    if split:
        cols["b"] = _terms_col(split, "y0")
        order.append("b")
        # Java/Micrometer fills absent tags with "none"; keep it out of breakdowns.
        kql = f'{kql} and not {split} : "none"' if kql else f'not {split} : "none"'
    if chart != "metric":
        order.append("x")
    order += accessors

    if chart == "metric":
        viz_type = "lnsMetric"
        visualization = {"layerId": layer, "layerType": "data", "metricAccessor": "y0"}
    else:
        viz_type = "lnsXY"
        layer_viz = {
            "layerId": layer,
            "layerType": "data",
            "seriesType": {"line": "line", "bar": "bar_stacked", "area": "area"}[chart],
            "xAccessor": "x",
            "accessors": accessors,
        }
        if split:
            layer_viz["splitAccessor"] = "b"
        visualization = {
            "preferredSeriesType": layer_viz["seriesType"],
            "legend": {"isVisible": True, "position": "bottom"},
            "valueLabels": "hide",
            "fittingFunction": "Linear",
            "emphasizeFitting": True,
            "endValue": "None",
            "layers": [layer_viz],
        }

    vid = oid(title)
    return {
        "id": vid,
        "type": "lens",
        "attributes": {
            "title": title,
            "description": description,
            "visualizationType": viz_type,
            "state": {
                "datasourceStates": {
                    "formBased": {"layers": {layer: {"columns": cols, "columnOrder": order, "incompleteColumns": {}}}}
                },
                "visualization": visualization,
                "query": {"query": kql, "language": "kuery"},
                "filters": [],
                "internalReferences": [],
                "adHocDataViews": {},
            },
        },
        "references": [
            {"type": "index-pattern", "id": DATA_VIEW_ID, "name": f"indexpattern-datasource-layer-{layer}"}
        ],
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "8.9.0",
    }


def controls(fields):
    panels = {}
    for i, (field, title) in enumerate(fields):
        cid = f"ctl{i}"
        panels[cid] = {
            "type": "optionsListControl",
            "order": i,
            "grow": True,
            "width": "medium",
            "explicitInput": {"id": cid, "fieldName": field, "title": title, "dataViewId": DATA_VIEW_ID},
        }
    return {
        "controlStyle": "oneLine",
        "chainingSystem": "HIERARCHICAL",
        "showApplySelections": False,
        "panelsJSON": json.dumps(panels),
        "ignoreParentSettingsJSON": json.dumps(
            {"ignoreFilters": False, "ignoreQuery": False, "ignoreTimerange": False, "ignoreValidations": False}
        ),
    }


def dashboard(title, description, rows, ctl):
    """rows: list of rows; each row is a list of (viz, width). Heights: metric rows 6, charts 12."""
    panels, refs, y = [], [], 0
    for row in rows:
        x, h = 0, 6 if all(v["attributes"]["visualizationType"] == "lnsMetric" for v, _ in row) else 12
        for viz, w in row:
            idx = str(len(panels))
            panels.append(
                {
                    "type": "lens",
                    "gridData": {"x": x, "y": y, "w": w, "h": h, "i": idx},
                    "panelIndex": idx,
                    "embeddableConfig": {"enhancements": {}},
                    "panelRefName": f"panel_{idx}",
                }
            )
            refs.append({"name": f"panel_{idx}", "type": "lens", "id": viz["id"]})
            x += w
        y += h
    return {
        "id": oid("dashboard:" + title),
        "type": "dashboard",
        "attributes": {
            "title": title,
            "description": description,
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({"useMargins": True, "syncColors": True, "syncCursor": True, "syncTooltips": True, "hidePanelTitles": False}),
            "timeRestore": True,
            "timeFrom": "now-1h",
            "timeTo": "now",
            "refreshInterval": {"pause": False, "value": 30000},
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"}, "filter": []})},
            "controlGroupInput": ctl,
        },
        "references": refs,
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "10.2.0",
    }


EVENT_ONLY = ("fail", "error", "throttl", "exhausted", "timeout", "terminate", "miss", "no_poller")


def runtime_shims(vizs):
    """Event-only counters (failures, throttling, cache misses ...) are not in the index mapping
    until the first event happens, and Lens rejects panels on unknown fields. A data-view runtime
    field with the same name returns the stored value when present and nothing otherwise, so those
    panels render (empty) on a fresh cluster and fill in once the metric appears."""
    fields = set()
    for viz in vizs:
        for layer in viz["attributes"]["state"]["datasourceStates"]["formBased"]["layers"].values():
            for col in layer["columns"].values():
                f = col.get("sourceField", "")
                if f.startswith(P) and not f.endswith(".histogram") and any(k in f for k in EVENT_ONLY):
                    fields.add(f)
    # Read from _source, not doc[...]: a runtime field that reads the mapped field of the same name
    # is a cyclic dependency in Elasticsearch once that field exists.
    src = (
        "def p = params._source.prometheus; if (p != null) { def m = p['%s']; "
        "if (m != null && m['%s'] != null) { emit(((Number) m['%s']).doubleValue()); } }"
    )
    shims = {}
    for f in sorted(fields):
        _, metric, sub = f.split(".", 2)
        shims[f] = {"type": "double", "script": {"source": src % (metric, sub, sub)}}
    return shims


def data_view(vizs):
    return {
        "id": DATA_VIEW_ID,
        "type": "index-pattern",
        "attributes": {
            "title": "metricbeat-*",
            "timeFieldName": "@timestamp",
            "name": "Temporal metrics (metricbeat-*)",
            "runtimeFieldMap": json.dumps(runtime_shims(vizs)),
        },
        "references": [],
        "coreMigrationVersion": "8.8.0",
        "typeMigrationVersion": "8.0.0",
    }


# ---------------------------------------------------------------- panels
def h(name):  # SDK histogram field
    return f"{P}{name}_seconds.histogram"


def r(name):  # SDK counter delta field
    return f"{P}{name}_total.rate"


def g(name):  # gauge field (SDK or Cloud)
    return f"{P}{name}.value"


def c(name):  # Cloud metric
    return g(f"temporal_cloud_v1_{name}")


def build():
    v = {}

    def add(key, viz):
        v[key] = viz
        return viz

    # ---- Overview: the capacity questions (is the worker keeping up? where is the latency?)
    add("ov_open", lens("Open workflows, peak (Cloud)", "Peak of temporal_cloud_v1_namespace_open_workflows in the selected time range.",
                        [("Open workflows", "max", c("namespace_open_workflows"))], kql=CLOUD, chart="metric"))
    add("ov_backlog", lens("Task backlog, peak (Cloud)", "Peak in the selected time range of temporal_cloud_v1_approximate_backlog_count across task queues. Sustained > 0 means workers are not keeping up.",
                           [("Backlog", "max", c("approximate_backlog_count"))], kql=CLOUD, chart="metric"))
    add("ov_wft_s2s", lens("Workflow task schedule-to-start p95 (SDK)", "p95 over the selected time range. Time a workflow task waits in Temporal Cloud before a worker picks it up.",
                           [("p95", "percentile", h("temporal_workflow_task_schedule_to_start_latency"))], kql=SDK, chart="metric", unit="s"))
    add("ov_act_s2s", lens("Activity schedule-to-start p95 (SDK)", "p95 over the selected time range. Time an activity task waits in Temporal Cloud before a worker picks it up.",
                           [("p95", "percentile", h("temporal_activity_schedule_to_start_latency"))], kql=SDK, chart="metric", unit="s"))
    add("ov_slots", lens("Min activity slots available (SDK)", "Over the selected time range. Lowest free activity-execution slot count across workers. 0 = saturated.",
                         [("Min slots", "min", g("temporal_worker_task_slots_available"))], kql=SDK + " and prometheus.labels.worker_type : ActivityWorker", chart="metric"))

    add("ov_wft_s2s_t", lens("Workflow task schedule-to-start p95 by SDK", "Cloud→worker latency for workflow tasks (temporal_workflow_task_schedule_to_start_latency). Rising values = add workers/pollers.",
                             [("p95", "percentile", h("temporal_workflow_task_schedule_to_start_latency"))], split=L + "sdk", kql=SDK, unit="s"))
    add("ov_act_s2s_t", lens("Activity schedule-to-start p95 by SDK", "Cloud→worker latency for activity tasks (temporal_activity_schedule_to_start_latency). Rising values = activity slots or pollers exhausted.",
                             [("p95", "percentile", h("temporal_activity_schedule_to_start_latency"))], split=L + "sdk", kql=SDK, unit="s"))
    add("ov_backlog_t", lens("Backlog count by task queue (Cloud)", "temporal_cloud_v1_approximate_backlog_count. Cloud data lags ~3 minutes.",
                             [("Backlog", "max", c("approximate_backlog_count"))], split=L + "temporal_task_queue", kql=CLOUD))
    add("ov_backlog_age", lens("Backlog age by task queue (Cloud)", "temporal_cloud_v1_approximate_backlog_age (seconds): age of the oldest waiting task.",
                               [("Age", "max", c("approximate_backlog_age"))], split=L + "temporal_task_queue", kql=CLOUD, unit="s"))
    add("ov_act_slots", lens("Activity slots available by SDK", "temporal_worker_task_slots_available{worker_type=ActivityWorker}, minimum per interval.",
                             [("Available", "min", g("temporal_worker_task_slots_available"))], split=L + "sdk", kql=SDK + " and prometheus.labels.worker_type : ActivityWorker"))
    add("ov_wf_slots", lens("Workflow slots available by SDK", "temporal_worker_task_slots_available{worker_type=WorkflowWorker}, minimum per interval.",
                            [("Available", "min", g("temporal_worker_task_slots_available"))], split=L + "sdk", kql=SDK + " and prometheus.labels.worker_type : WorkflowWorker"))
    add("ov_sync", lens("Poll success vs sync match (Cloud)", "temporal_cloud_v1_poll_success_count vs poll_success_sync_count (per second). Sync ≈ success means tasks are handed straight to waiting pollers; a widening gap means tasks are queuing.",
                        [("Poll success /s", "average", c("poll_success_count")), ("Sync match /s", "average", c("poll_success_sync_count"))], kql=CLOUD))
    add("ov_wf_outcome", lens("Workflows completed / failed (SDK)", "temporal_workflow_completed_total and temporal_workflow_failed_total, count per interval.",
                              [("Completed", "sum", r("temporal_workflow_completed")), ("Failed", "sum", r("temporal_workflow_failed"))], kql=SDK, chart="bar"))
    add("ov_wf_e2e", lens("Workflow end-to-end p95 by type (SDK)", "temporal_workflow_endtoend_latency, p95.",
                          [("p95", "percentile", h("temporal_workflow_endtoend_latency"))], split=L + "workflow_type", kql=SDK, unit="s"))
    add("ov_act_exec", lens("Activity execution p95 by type (SDK)", "temporal_activity_execution_latency, p95 per activity type.",
                            [("p95", "percentile", h("temporal_activity_execution_latency"))], split=L + "activity_type", kql=SDK, unit="s"))
    add("ov_act_fail", lens("Activity failures by type (SDK)", "temporal_activity_execution_failed_total, count per interval (includes attempts that are later retried).",
                            [("Failures", "sum", r("temporal_activity_execution_failed"))], split=L + "activity_type", kql=SDK, chart="bar"))

    # ---- SDK detail
    add("sdk_req", lens("Requests by operation", "temporal_request_total (worker→Cloud gRPC calls), count per interval.",
                        [("Requests", "sum", r("temporal_request"))], split=L + "operation", kql=SDK, chart="bar"))
    add("sdk_req_fail", lens("Request failures by operation", "temporal_request_failure_total, count per interval.",
                             [("Failures", "sum", r("temporal_request_failure"))], split=L + "operation", kql=SDK, chart="bar"))
    add("sdk_req_lat", lens("Request latency p95 by SDK", "temporal_request_latency (excludes long polls).",
                            [("p95", "percentile", h("temporal_request_latency"))], split=L + "sdk", kql=SDK, unit="s"))
    add("sdk_poll_lat", lens("Long-poll latency p95 by SDK", "temporal_long_request_latency. Long polls near the poll timeout mean workers are idle; short ones mean tasks are waiting.",
                             [("p95", "percentile", h("temporal_long_request_latency"))], split=L + "sdk", kql=SDK, unit="s"))
    add("sdk_wft_exec", lens("Workflow task execution p95 by SDK", "temporal_workflow_task_execution_latency: time spent running workflow code per task.",
                             [("p95", "percentile", h("temporal_workflow_task_execution_latency"))], split=L + "sdk", kql=SDK, unit="s"))
    add("sdk_replay", lens("Workflow task replay p95 by SDK", "temporal_workflow_task_replay_latency: grows with history size and cache misses.",
                           [("p95", "percentile", h("temporal_workflow_task_replay_latency"))], split=L + "sdk", kql=SDK, unit="s"))
    add("sdk_wft_fail", lens("Workflow task failures", "temporal_workflow_task_execution_failed_total (non-determinism, panics, bugs), count per interval.",
                             [("Failures", "sum", r("temporal_workflow_task_execution_failed"))], split=L + "sdk", kql=SDK, chart="bar"))
    add("sdk_act_e2e", lens("Activity succeeded end-to-end p95 by type", "temporal_activity_succeed_endtoend_latency: schedule to final success, including retries.",
                            [("p95", "percentile", h("temporal_activity_succeed_endtoend_latency"))], split=L + "activity_type", kql=SDK, unit="s"))
    add("sdk_slots_used", lens("Slots used by worker type", "temporal_worker_task_slots_used, max per interval.",
                               [("Used", "max", g("temporal_worker_task_slots_used"))], split=L + "worker_type", kql=SDK))
    add("sdk_pollers", lens("Pollers by type", "temporal_num_pollers, max per interval.",
                            [("Pollers", "max", g("temporal_num_pollers"))], split=L + "poller_type", kql=SDK))
    add("sdk_sticky", lens("Sticky cache size by SDK", "temporal_sticky_cache_size: cached workflow executions.",
                           [("Size", "max", g("temporal_sticky_cache_size"))], split=L + "sdk", kql=SDK))
    add("sdk_sticky_hit", lens("Sticky cache hits / misses", "temporal_sticky_cache_hit_total vs temporal_sticky_cache_miss_total, count per interval. Misses force a full replay.",
                               [("Hits", "sum", r("temporal_sticky_cache_hit")), ("Misses", "sum", r("temporal_sticky_cache_miss"))], kql=SDK, chart="bar"))
    add("sdk_threads", lens("Java workflow threads", "temporal_workflow_active_thread_count (Java only).",
                            [("Threads", "max", g("temporal_workflow_active_thread_count"))], kql=SDK + " and prometheus.labels.sdk : java"))

    # ---- Cloud detail
    add("cl_open_t", lens("Open workflows over time", "temporal_cloud_v1_namespace_open_workflows (gauge).",
                          [("Open workflows", "max", c("namespace_open_workflows"))], kql=CLOUD))
    add("cl_wf_ok", lens("Workflow success /s by type", "temporal_cloud_v1_workflow_success_count (per-second rate, 1-min average).",
                         [("Success /s", "average", c("workflow_success_count"))], split=L + "temporal_workflow_type", kql=CLOUD))
    add("cl_wf_bad", lens("Workflow failed / timed out / terminated /s", "temporal_cloud_v1_workflow_failed_count, workflow_timeout_count, workflow_terminate_count.",
                          [("Failed", "average", c("workflow_failed_count")), ("Timeout", "average", c("workflow_timeout_count")), ("Terminated", "average", c("workflow_terminate_count"))], kql=CLOUD, chart="bar"))
    add("cl_wf_lat", lens("Workflow schedule-to-close p95 by type", "temporal_cloud_v1_workflow_schedule_to_close_latency_p95 (precomputed by Cloud; do not re-aggregate).",
                          [("p95", "max", c("workflow_schedule_to_close_latency_p95"))], split=L + "temporal_workflow_type", kql=CLOUD, unit="s"))
    add("cl_act", lens("Activity success / fail /s", "temporal_cloud_v1_activity_success_count and activity_fail_count.",
                       [("Success", "average", c("activity_success_count")), ("Fail", "average", c("activity_fail_count"))], kql=CLOUD))
    add("cl_act_lat", lens("Activity start-to-close p95 by type", "temporal_cloud_v1_activity_start_to_close_latency_p95 (needs labels=temporal_activity_type in the scrape).",
                           [("p95", "max", c("activity_start_to_close_latency_p95"))], split=L + "temporal_activity_type", kql=CLOUD, unit="s"))
    add("cl_poll", lens("Polls: success, timeout, no-poller tasks", "temporal_cloud_v1_poll_success_count, poll_timeout_count, no_poller_tasks_count. no_poller_tasks > 0 means tasks arrived with no worker polling.",
                        [("Success", "average", c("poll_success_count")), ("Timeout", "average", c("poll_timeout_count")), ("No poller", "average", c("no_poller_tasks_count"))], kql=CLOUD))
    add("cl_req", lens("Service requests /s by operation", "temporal_cloud_v1_service_request_count.",
                       [("Requests /s", "average", c("service_request_count"))], split=L + "operation", kql=CLOUD, chart="area"))
    add("cl_err", lens("Service errors /s by operation", "temporal_cloud_v1_service_error_count.",
                       [("Errors /s", "average", c("service_error_count"))], split=L + "operation", kql=CLOUD, chart="bar"))
    add("cl_lat", lens("Service latency p95 by operation", "temporal_cloud_v1_service_latency_p95.",
                       [("p95", "max", c("service_latency_p95"))], split=L + "operation", kql=CLOUD, unit="s"))
    add("cl_throttle", lens("Throttling", "temporal_cloud_v1_service_request_throttled_count, resource_exhausted_error_count, total_action_throttled_count (per second). Anything non-zero needs attention.",
                            [("Requests throttled", "average", c("service_request_throttled_count")), ("Resource exhausted", "average", c("resource_exhausted_error_count")), ("Actions throttled", "average", c("total_action_throttled_count"))], kql=CLOUD, chart="bar"))
    add("cl_actions", lens("Actions /s vs limit", "temporal_cloud_v1_total_action_count vs action_limit (actions per second).",
                           [("Actions /s", "average", c("total_action_count")), ("Limit", "max", c("action_limit"))], kql=CLOUD))
    add("cl_rps", lens("Requests /s vs limit", "Sum of temporal_cloud_v1_service_request_count across operations vs service_request_limit.",
                       [("Requests /s (sum of ops)", "sum", c("service_request_count")), ("Limit", "max", c("service_request_limit"))], kql=CLOUD))

    common_ctl = controls([(L + "sdk", "SDK"), (L + "task_queue", "Task queue (SDK)"), (L + "temporal_task_queue", "Task queue (Cloud)")])
    dashboards = [
        dashboard(
            "Temporal · Worker capacity overview",
            "Is the worker fleet keeping up? Cloud backlog + SDK schedule-to-start latency + slot usage, Java and Python side by side.",
            [
                [(v["ov_open"], 9), (v["ov_backlog"], 9), (v["ov_wft_s2s"], 10), (v["ov_act_s2s"], 10), (v["ov_slots"], 10)],
                [(v["ov_wft_s2s_t"], 24), (v["ov_act_s2s_t"], 24)],
                [(v["ov_backlog_t"], 24), (v["ov_backlog_age"], 24)],
                [(v["ov_act_slots"], 16), (v["ov_wf_slots"], 16), (v["ov_sync"], 16)],
                [(v["ov_wf_outcome"], 16), (v["ov_wf_e2e"], 16), (v["ov_act_exec"], 16)],
                [(v["ov_act_fail"], 48)],
            ],
            common_ctl,
        ),
        dashboard(
            "Temporal · SDK workers (Java + Python)",
            "Worker-side SDK metrics scraped from each worker's /metrics endpoint. Mirrors the panels of Temporal's reference SDK dashboard.",
            [
                [(v["sdk_req"], 16), (v["sdk_req_fail"], 16), (v["sdk_req_lat"], 16)],
                [(v["sdk_poll_lat"], 16), (v["sdk_wft_exec"], 16), (v["sdk_replay"], 16)],
                [(v["sdk_wft_fail"], 16), (v["ov_act_fail"], 16), (v["sdk_act_e2e"], 16)],
                [(v["sdk_slots_used"], 16), (v["sdk_pollers"], 16), (v["sdk_sticky"], 16)],
                [(v["sdk_sticky_hit"], 24), (v["sdk_threads"], 24)],
            ],
            common_ctl,
        ),
        dashboard(
            "Temporal · Cloud namespace health",
            "Temporal Cloud OpenMetrics (metrics.temporal.io/v1/metrics). All series are gauges; *_count are per-second rates averaged per minute; data lags ~3 minutes.",
            [
                [(v["ov_open"], 12), (v["ov_backlog"], 12)],
                [(v["cl_open_t"], 48)],
                [(v["cl_wf_ok"], 16), (v["cl_wf_bad"], 16), (v["cl_wf_lat"], 16)],
                [(v["cl_act"], 16), (v["cl_act_lat"], 16), (v["cl_poll"], 16)],
                [(v["ov_backlog_t"], 24), (v["ov_backlog_age"], 24)],
                [(v["cl_req"], 16), (v["cl_err"], 16), (v["cl_lat"], 16)],
                [(v["cl_throttle"], 16), (v["cl_actions"], 16), (v["cl_rps"], 16)],
            ],
            controls([(L + "temporal_namespace", "Namespace"), (L + "temporal_task_queue", "Task queue")]),
        ),
    ]
    return [data_view(list(v.values())), *v.values(), *dashboards]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(pathlib.Path(__file__).with_name("temporal-dashboards.ndjson")))
    ap.add_argument("--import", dest="kibana", help="Kibana URL to import into, e.g. http://localhost:5601")
    a = ap.parse_args()
    objs = build()
    body = "\n".join(json.dumps(o, separators=(",", ":")) for o in objs) + "\n"
    pathlib.Path(a.out).write_text(body)
    print(f"wrote {len(objs)} saved objects to {a.out}")
    if a.kibana:
        import_ndjson(a.kibana, a.out)


def import_ndjson(kibana, path):
    boundary = "----tckboundary"
    data = pathlib.Path(path).read_bytes()
    payload = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{pathlib.Path(path).name}\"\r\n"
        f"Content-Type: application/ndjson\r\n\r\n"
    ).encode() + data + f"\r\n--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        f"{kibana.rstrip('/')}/api/saved_objects/_import?overwrite=true",
        data=payload,
        headers={"kbn-xsrf": "true", "Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        res = json.load(resp)
    print("import:", {k: res.get(k) for k in ("success", "successCount")})
    for err in res.get("errors", []):
        print("  error:", err.get("id"), err.get("type"), err.get("error"))


if __name__ == "__main__":
    main()
