"""
Central configuration for the Dual-Space Architecture pipeline.

Each flag corresponds to a research phase and can be toggled independently
for ablation studies.
"""

from dataclasses import dataclass, field


@dataclass
class PipelineConfig:
    # Phase 1: Constraint Formalization
    enable_constraint_extraction: bool = True

    # Phase 2: Inner Space Solver (OR-Tools CP-SAT)
    enable_solver: bool = False
    solver_time_limit_seconds: float = 30.0
    solver_fallback_to_assemble_day: bool = True

    # Phase 3: TSPTW Route Optimization
    enable_tsptw: bool = False
    tsptw_max_nodes_per_day: int = 8

    # Phase 4: Outer-Inner Feedback Loop
    enable_solver_feedback: bool = False
    max_solver_retries: int = 3

    # Phase 5: VoI-Guided Query Strategy
    enable_voi: bool = False

    # Phase 6: Fuzzy Constraint Handling
    enable_fuzzy: bool = False
    fuzzy_initial_threshold: str = "conservative"  # "conservative" | "midpoint" | "relaxed"
