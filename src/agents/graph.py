"""Sequential and LangGraph-compatible orchestration for the copilot."""

from __future__ import annotations

from typing import Any

from src.agents.classifier import QueryClassification, classify_query
from src.agents.filter_builder import build_filters
from src.agents.formatter import CopilotResponse, format_response
from src.agents.medical_reasoner import MedicalReasoner
from src.processing.specialty_mapper import SpecialtyMapper
from src.retrieval.hybrid_router import HybridRetrievalEngine, RetrievalRequest
from src.utils.mlflow_utils import SafeMLflowTracker, TracePayload


class VirtueHealthGraph:
    """Main orchestration layer for classification, retrieval, and reasoning."""

    def __init__(
        self,
        retrieval_engine: HybridRetrievalEngine,
        mapper: SpecialtyMapper | None = None,
        tracker: SafeMLflowTracker | None = None,
        known_regions: list[str] | None = None,
        known_names: list[str] | None = None,
        known_facility_types: list[str] | None = None,
        known_operator_types: list[str] | None = None,
        region_rows: list[dict[str, Any]] | None = None,
    ) -> None:
        self.retrieval_engine = retrieval_engine
        self.mapper = mapper or SpecialtyMapper()
        self.tracker = tracker or SafeMLflowTracker(enabled=False)
        self.known_regions = known_regions or []
        self.known_names = known_names or []
        self.known_facility_types = known_facility_types or []
        self.known_operator_types = known_operator_types or []
        self.reasoner = MedicalReasoner(region_rows=region_rows)
        self.compiled_graph = self._compile_langgraph()

    def _compile_langgraph(self) -> Any | None:
        """Build an optional LangGraph DAG when the dependency is available."""

        try:
            from langgraph.graph import END, StateGraph
        except Exception:
            return None

        builder = StateGraph(dict)
        builder.add_node("query_classifier", self.query_classifier_node)
        builder.add_node("filter_builder", self.filter_builder_node)
        builder.add_node("retrieval_router", self.retrieval_router_node)
        builder.add_node("medical_reasoning", self.medical_reasoning_node)
        builder.add_node("evidence_assembler", self.evidence_assembler_node)
        builder.add_node("response_formatter", self.response_formatter_node)
        builder.set_entry_point("query_classifier")
        builder.add_edge("query_classifier", "filter_builder")
        builder.add_edge("filter_builder", "retrieval_router")
        builder.add_edge("retrieval_router", "medical_reasoning")
        builder.add_edge("medical_reasoning", "evidence_assembler")
        builder.add_edge("evidence_assembler", "response_formatter")
        builder.add_edge("response_formatter", END)
        return builder.compile()

    def query_classifier_node(self, state: dict[str, Any]) -> dict[str, Any]:
        classification = classify_query(state["query"])
        return {"classification": classification}

    def filter_builder_node(self, state: dict[str, Any]) -> dict[str, Any]:
        filters = build_filters(
            state["query"],
            mapper=self.mapper,
            known_regions=self.known_regions,
            known_facility_types=self.known_facility_types,
            known_operator_types=self.known_operator_types,
            known_names=self.known_names,
        ).to_dict()
        return {"filters": filters}

    def retrieval_router_node(self, state: dict[str, Any]) -> dict[str, Any]:
        request = RetrievalRequest(
            query_text=state["query"],
            query_family=state["classification"].family,
            filters=state["filters"],
        )
        retrieval_result = self.retrieval_engine.retrieve(request)
        return {"retrieval_result": retrieval_result}

    def medical_reasoning_node(self, state: dict[str, Any]) -> dict[str, Any]:
        reasoned = self.reasoner.reason(
            state["query"],
            state["classification"],
            state["filters"],
            state["retrieval_result"],
        )
        return {"reasoned": reasoned}

    def evidence_assembler_node(self, state: dict[str, Any]) -> dict[str, Any]:
        evidence = state["reasoned"].get("evidence", [])
        normalized_evidence: list[dict[str, Any]] = []
        for row in evidence:
            if not isinstance(row, dict):
                continue
            normalized_evidence.append(row)
        updated_reasoned = dict(state["reasoned"])
        updated_reasoned["evidence"] = normalized_evidence
        return {"reasoned": updated_reasoned}

    def response_formatter_node(self, state: dict[str, Any]) -> dict[str, Any]:
        response = format_response(
            classification=state["classification"],
            filters=state["filters"],
            retrieval_result=state["retrieval_result"],
            reasoned=state["reasoned"],
        )
        return {"response": response}

    def _run_sequential(self, query: str) -> CopilotResponse:
        state: dict[str, Any] = {"query": query}
        state.update(self.query_classifier_node(state))
        state.update(self.filter_builder_node(state))
        state.update(self.retrieval_router_node(state))
        state.update(self.medical_reasoning_node(state))
        state.update(self.evidence_assembler_node(state))
        state.update(self.response_formatter_node(state))
        return state["response"]

    def run(self, query: str) -> CopilotResponse:
        """Execute the full agentic workflow and emit safe MLflow traces."""

        if self.tracker and getattr(self.tracker, "enabled", False):
            self.tracker.start_run(run_name="virtue_health_planning_query")
        if self.compiled_graph is not None:
            state = self.compiled_graph.invoke({"query": query})
            response = state["response"]
            classification: QueryClassification = state["classification"]
            filters = state["filters"]
            retrieval_result = state["retrieval_result"]
        else:
            response = self._run_sequential(query)
            classification = classify_query(query)
            filters = build_filters(
                query,
                mapper=self.mapper,
                known_regions=self.known_regions,
                known_facility_types=self.known_facility_types,
                known_operator_types=self.known_operator_types,
                known_names=self.known_names,
            ).to_dict()
            retrieval_result = self.retrieval_engine.retrieve(
                RetrievalRequest(query_text=query, query_family=classification.family, filters=filters)
            )

        self.tracker.log_trace(
            TracePayload(
                user_query=query,
                classified_intent=classification.family,
                extracted_filters=filters,
                retrieval_path=response.retrieval_path,
                sql_used=response.sql_used,
                retrieved_fact_rows=response.evidence[:10],
                anomaly_rules_triggered=response.anomaly_cards,
                final_answer_text=response.summary,
            )
        )
        if self.tracker and getattr(self.tracker, "enabled", False):
            self.tracker.end_run()
        return response
