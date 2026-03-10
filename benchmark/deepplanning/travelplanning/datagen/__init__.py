"""
DeepPlanning Travel Planning Data Construction Pipeline.

Generates new travel planning tasks by forking existing databases,
constructing valid solutions, injecting constraints, and generating queries.

Usage:
    python -m travelplanning.datagen.orchestrator --num-tasks 10 --workers 1
"""
