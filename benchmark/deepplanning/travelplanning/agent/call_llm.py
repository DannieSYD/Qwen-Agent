"""
Universal LLM calling module
Supports multiple providers: OpenAI, Anthropic (Claude), Google (Gemini), etc.
"""
import json
import os
import time
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional

import openai
from openai.types.chat import ChatCompletion, ChatCompletionMessage
from openai.types.chat.chat_completion import Choice
from openai.types.chat.chat_completion_message_tool_call import ChatCompletionMessageToolCall, Function


def load_model_config(model_name: str) -> Dict[str, Any]:
    """
    Load model configuration from models_config.json
    
    Searches for models_config.json in the following order:
    1. Current domain directory (travelplanning/)
    2. Parent directory (project root)
    
    Args:
        model_name: Name of the model
        
    Returns:
        Model configuration dict
        
    Raises:
        FileNotFoundError: If config file not found
        ValueError: If model not found in config
    """
    # Try domain directory first
    domain_config_path = Path(__file__).parent.parent / 'models_config.json'
    # Try project root (parent of domain directory)
    root_config_path = Path(__file__).parent.parent.parent / 'models_config.json'
    
    config_path = None
    if domain_config_path.exists():
        config_path = domain_config_path
    elif root_config_path.exists():
        config_path = root_config_path
    else:
        raise FileNotFoundError(
            f"models_config.json not found in:\n"
            f"  - Domain directory: {domain_config_path}\n"
            f"  - Project root: {root_config_path}\n"
            f"Please create models_config.json in the project root or domain directory."
        )
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    models = config.get('models', {})
    if model_name not in models:
        available = ', '.join(models.keys())
        raise ValueError(
            f"Model '{model_name}' not found in models_config.json\n"
            f"Available models: {available}"
        )
    
    return models[model_name]


def create_client(model_name: str, model_config: Optional[Dict[str, Any]] = None):
    """
    Create appropriate client based on model configuration
    
    Args:
        model_name: Name of the model
        model_config: Model configuration (if None, will load from config file)
        
    Returns:
        Initialized client instance
    """
    if model_config is None:
        model_config = load_model_config(model_name)
    
    model_type = model_config.get('model_type', 'openai')
    base_url = model_config['base_url']
    api_key_env = model_config.get('api_key_env')
    api_key = os.getenv(api_key_env) if api_key_env else None
    
    if not api_key:
        raise RuntimeError(
            f"API key not found for model '{model_name}'\n"
            f"Please set environment variable: {api_key_env}"
        )
    
    if model_type == 'openai':
        # OpenAI and OpenAI-compatible APIs (Qwen, DeepSeek, etc.)
        return openai.OpenAI(api_key=api_key, base_url=base_url)
    else:
        raise NotImplementedError(
            f"Model type '{model_type}' is not currently supported. "
            f"Supported types: openai"
        )


def call_llm(
    config_name: str,
    messages: List[Dict[str, Any]],
    tools: Optional[List[Dict[str, Any]]] = None
):
    """
    Universal LLM call with automatic client creation and retry logic
    
    Args:
        config_name: Configuration name from models_config.json (display name)
        messages: Message list
        tools: Tool definitions (optional)
    
    Returns:
        API response object
        
    Note:
        All parameters (model_name, temperature, extra_body, etc.) are loaded
        from models_config.json based on the config_name.
    """
    # Load model config and create client
    model_config = load_model_config(config_name)
    client = create_client(config_name, model_config)
    
    # Get actual model name for API call (fallback to config_name if not specified)
    actual_model_name = model_config.get('model_name', config_name)
    
    # Get parameters from config or use defaults
    temperature = model_config.get('temperature', None)
    max_retries = model_config.get('max_retries', 30)
    backoff = model_config.get('backoff', 1.5)
    tool_choice = model_config.get('tool_choice', 'auto')
    extra_body = model_config.get('extra_body')  # Get from config
    
    # Detect reasoning models (don't support temperature)
    is_reasoning_model = any(x in actual_model_name.lower() for x in ['o1', 'o3', 'o4-mini', 'reasoner'])

    # Determine if we need the Responses API:
    # Some models (e.g. gpt-5.4) don't support reasoning_effort + tools on /v1/chat/completions
    reasoning_effort = extra_body.get('reasoning_effort') if extra_body else None
    use_responses_api = bool(reasoning_effort and tools)

    last_err = None

    for attempt in range(max_retries):
        try:
            if use_responses_api:
                response = _call_responses_api(
                    client, actual_model_name, messages, tools,
                    reasoning_effort,
                )
            else:
                params = {
                    "model": actual_model_name,
                    "messages": messages,
                }

                if tools:
                    params["tools"] = tools

                if not is_reasoning_model and not reasoning_effort and temperature:
                    params["temperature"] = temperature

                if extra_body:
                    params["extra_body"] = extra_body
                response = client.chat.completions.create(**params)

            # Validate response
            msg = response.choices[0].message
            has_content = msg.content and msg.content.strip()
            has_tool_calls = hasattr(msg, 'tool_calls') and msg.tool_calls

            if not has_content and not has_tool_calls:
                raise ValueError("Model returned an empty response without tool calls")

            return response

        except Exception as e:
            last_err = e

            if attempt == max_retries - 1:
                raise

            wait_time = backoff
            print(f"  ⚠️  LLM API error (attempt {attempt + 1}/{max_retries}): {e}")
            print(f"     Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)

    raise last_err if last_err else RuntimeError("LLM API call failed")


def _call_responses_api(client, model, messages, tools, reasoning_effort):
    """
    Call OpenAI Responses API and convert result to ChatCompletion format
    so the rest of the codebase doesn't need changes.
    """
    # Build tools in responses API format (same schema, wrapped with type)
    resp_tools = []
    for t in tools:
        resp_tools.append({
            "type": "function",
            "name": t["function"]["name"],
            "description": t["function"].get("description", ""),
            "parameters": t["function"].get("parameters", {}),
        })

    # Convert chat completions messages to Responses API input format.
    # Key differences:
    #   assistant + tool_calls → separate {"type": "function_call", ...} items
    #   {"role": "tool", ...}  → {"type": "function_call_output", ...}
    resp_input = []
    for m in messages:
        # Normalize to dict
        if not isinstance(m, dict):
            md = {"role": getattr(m, 'role', 'assistant'),
                  "content": m.content}
            if hasattr(m, 'tool_calls') and m.tool_calls:
                md["tool_calls"] = [
                    {"id": tc.id, "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
                    for tc in m.tool_calls
                ]
            m = md

        role = m.get("role")

        if role == "tool":
            # Tool result → function_call_output
            resp_input.append({
                "type": "function_call_output",
                "call_id": m["tool_call_id"],
                "output": m.get("content") or "",
            })
        elif role == "assistant":
            # If assistant has text content, emit as a message
            if m.get("content"):
                resp_input.append({"role": "assistant", "content": m["content"]})
            # Each tool call becomes a separate function_call item
            for tc in (m.get("tool_calls") or []):
                func = tc.get("function", {}) if isinstance(tc, dict) else tc.function
                tc_id = tc.get("id") if isinstance(tc, dict) else tc.id
                func_name = func.get("name") if isinstance(func, dict) else func.name
                func_args = func.get("arguments") if isinstance(func, dict) else func.arguments
                resp_input.append({
                    "type": "function_call",
                    "call_id": tc_id,
                    "name": func_name,
                    "arguments": func_args,
                })
        else:
            # system / user messages pass through
            entry = dict(m)
            if entry.get("content") is None:
                entry["content"] = ""
            resp_input.append(entry)

    params = {
        "model": model,
        "input": resp_input,
        "tools": resp_tools,
        "reasoning": {"effort": reasoning_effort},
    }
    resp = client.responses.create(**params)

    # Convert responses API output to ChatCompletion format
    content_parts = []
    tool_calls = []
    for item in resp.output:
        if item.type == "message":
            for c in item.content:
                if hasattr(c, 'text'):
                    content_parts.append(c.text)
        elif item.type == "function_call":
            tool_calls.append(ChatCompletionMessageToolCall(
                id=item.call_id,
                type="function",
                function=Function(
                    name=item.name,
                    arguments=item.arguments,
                ),
            ))

    content_text = "\n".join(content_parts) if content_parts else None

    message = ChatCompletionMessage(
        role="assistant",
        content=content_text,
        tool_calls=tool_calls if tool_calls else None,
    )

    choice = Choice(index=0, message=message, finish_reason="stop")

    return ChatCompletion(
        id=resp.id or f"resp-{uuid.uuid4().hex[:12]}",
        object="chat.completion",
        created=int(time.time()),
        model=model,
        choices=[choice],
    )
