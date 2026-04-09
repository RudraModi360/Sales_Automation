import os
import sys
import time
from typing import Any

from dotenv import load_dotenv
import ollama


def _build_client() -> tuple[Any, str, str]:
    load_dotenv()

    api_key = (os.getenv('OLLAMA_API_KEY') or '').strip()
    host = (os.getenv('OLLAMA_HOST') or '').strip()
    model = (
        (os.getenv('OLLAMA_EMAIL_MODEL') or '').strip()
        or (os.getenv('OLLAMA_MODEL') or '').strip()
        or 'gpt-oss:20b-cloud'
    )

    if api_key:
        client = ollama.Client(host=host or 'https://ollama.com', headers={'Authorization': f'Bearer {api_key}'})
    elif host:
        client = ollama.Client(host=host)
    else:
        client = ollama.Client()

    return client, host or 'https://ollama.com', model


def _as_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, 'model_dump'):
        dumped = obj.model_dump()
        if isinstance(dumped, dict):
            return dumped
    return {
        'response': getattr(obj, 'response', ''),
        'context': getattr(obj, 'context', None),
        'prompt_eval_count': getattr(obj, 'prompt_eval_count', None),
        'eval_count': getattr(obj, 'eval_count', None),
        'prompt_eval_duration': getattr(obj, 'prompt_eval_duration', None),
        'eval_duration': getattr(obj, 'eval_duration', None),
        'total_duration': getattr(obj, 'total_duration', None),
    }


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _fmt(value: Any) -> str:
    if value is None:
        return 'None'
    if isinstance(value, float):
        return f'{value:.3f}'
    return str(value)


def run_stream(client: Any, model: str, prompt: str, context: list[int] | None = None) -> tuple[dict[str, Any], list[int] | None]:
    start = time.perf_counter()
    first_chunk_time = None
    first_token_time = None

    chunk_count = 0
    empty_chunk_count = 0
    output_text = ''
    last_chunk: dict[str, Any] = {}
    latest_context: list[int] | None = None
    latest_prompt_eval_count: int | None = None
    latest_eval_count: int | None = None

    stream = client.generate(
        model=model,
        prompt=prompt,
        context=context,
        stream=True,
        options={'temperature': 0.0, 'num_predict': 96},
    )

    for raw in stream:
        now = time.perf_counter()
        chunk = _as_dict(raw)
        chunk_count += 1

        if first_chunk_time is None:
            first_chunk_time = now

        token = str(chunk.get('response') or '')
        if token:
            if first_token_time is None:
                first_token_time = now
            output_text += token
        else:
            empty_chunk_count += 1

        context_tokens = chunk.get('context')
        if isinstance(context_tokens, tuple):
            context_tokens = list(context_tokens)
        if isinstance(context_tokens, list):
            latest_context = context_tokens

        prompt_eval_count = chunk.get('prompt_eval_count')
        if prompt_eval_count is not None:
            latest_prompt_eval_count = _safe_int(prompt_eval_count)

        eval_count = chunk.get('eval_count')
        if eval_count is not None:
            latest_eval_count = _safe_int(eval_count)

        last_chunk = chunk

    end = time.perf_counter()
    returned_context = latest_context

    input_tokens = latest_prompt_eval_count if latest_prompt_eval_count is not None else _safe_int(last_chunk.get('prompt_eval_count'))
    output_tokens = latest_eval_count if latest_eval_count is not None else _safe_int(last_chunk.get('eval_count'))

    metrics = {
        'ttft_first_chunk_s': (first_chunk_time - start) if first_chunk_time else None,
        'ttft_first_token_s': (first_token_time - start) if first_token_time else None,
        'total_s': end - start,
        'chunk_count': chunk_count,
        'empty_chunk_count': empty_chunk_count,
        'prompt_eval_count': last_chunk.get('prompt_eval_count'),
        'eval_count': last_chunk.get('eval_count'),
        'input_tokens': input_tokens,
        'output_tokens': output_tokens,
        'total_tokens': input_tokens + output_tokens,
        'prompt_eval_duration_ns': last_chunk.get('prompt_eval_duration'),
        'eval_duration_ns': last_chunk.get('eval_duration'),
        'total_duration_ns': last_chunk.get('total_duration'),
        'context_available': returned_context is not None,
        'context_len': len(returned_context) if returned_context is not None else None,
        'output_preview': output_text[:120].replace('\n', ' '),
    }

    return metrics, returned_context


def print_metrics(label: str, metrics: dict[str, Any]) -> None:
    print('-' * 100)
    print(label)
    print(
        'ttft_first_chunk_s=', _fmt(metrics['ttft_first_chunk_s']),
        '| ttft_first_token_s=', _fmt(metrics['ttft_first_token_s']),
        '| total_s=', _fmt(metrics['total_s']),
    )
    print('chunks=', metrics['chunk_count'], '| empty_chunks=', metrics['empty_chunk_count'])
    print(
        'input_tokens=', metrics['input_tokens'],
        '| output_tokens=', metrics['output_tokens'],
        '| total_tokens=', metrics['total_tokens'],
    )
    print(
        'prompt_eval_count=', metrics['prompt_eval_count'],
        '| eval_count=', metrics['eval_count'],
        '| context_available=', metrics['context_available'],
        '| context_len=', metrics['context_len'],
    )
    print(
        'prompt_eval_duration_ns=', metrics['prompt_eval_duration_ns'],
        '| eval_duration_ns=', metrics['eval_duration_ns'],
        '| total_duration_ns=', metrics['total_duration_ns'],
    )
    print('preview=', metrics['output_preview'])


def main() -> int:
    client, host, model = _build_client()

    print('=' * 100)
    print('OLLAMA CACHE TEST LOOP')
    print('host=', host)
    print('model=', model)
    print('=' * 100)

    queries = [
        'What is one sentence about insurance technology?',
        'Now one sentence about operational efficiency.',
        'Now one sentence about risk reduction.',
    ]

    print('\n[A] NO-CACHE MODE (full growing prompt, no context)')
    history: list[str] = []
    no_cache_results: list[dict[str, Any]] = []

    for idx, query in enumerate(queries, start=1):
        history.append(f'User: {query}')
        prompt = '\n'.join(history)
        metrics, _ = run_stream(client=client, model=model, prompt=prompt, context=None)
        no_cache_results.append(metrics)
        print_metrics(f'A{idx}: no-cache', metrics)
        history.append('Assistant: ' + (metrics['output_preview'] or ''))

    print('\n[B] CACHE MODE (incremental prompt + returned context reuse)')
    context: list[int] | None = None
    first_context_seen = False
    cache_results: list[dict[str, Any]] = []

    for idx, query in enumerate(queries, start=1):
        metrics, context = run_stream(client=client, model=model, prompt=query, context=context)
        cache_results.append(metrics)
        print_metrics(f'B{idx}: cache', metrics)
        if metrics['context_available']:
            first_context_seen = True

    print('\n[C] ROUGH CACHED TOKEN ESTIMATE (B vs A by iteration)')
    total_no_cache_input = 0
    total_cache_input = 0
    total_rough_saved_input = 0

    for idx, (a_metrics, b_metrics) in enumerate(zip(no_cache_results, cache_results), start=1):
        no_cache_in = a_metrics['input_tokens']
        cache_in = b_metrics['input_tokens']
        rough_saved_input = max(0, no_cache_in - cache_in)
        rough_hit_rate = (rough_saved_input / no_cache_in * 100.0) if no_cache_in > 0 else 0.0

        total_no_cache_input += no_cache_in
        total_cache_input += cache_in
        total_rough_saved_input += rough_saved_input

        print(
            f'iter={idx} | no_cache_input={no_cache_in} | cache_input={cache_in} '
            f'| rough_saved_input={rough_saved_input} | rough_hit_rate={rough_hit_rate:.1f}% '
            f'| context_returned={b_metrics["context_available"]}'
        )

    total_rough_hit_rate = (total_rough_saved_input / total_no_cache_input * 100.0) if total_no_cache_input > 0 else 0.0

    print(
        'totals | '
        f'no_cache_input={total_no_cache_input} | '
        f'cache_input={total_cache_input} | '
        f'rough_saved_input={total_rough_saved_input} | '
        f'rough_hit_rate={total_rough_hit_rate:.1f}%'
    )

    print('\n' + '=' * 100)
    print('TELEMETRY NOTES')
    print('1) Correct TTFT is first non-empty token time (ttft_first_token_s).')
    print('2) input_tokens/output_tokens come from prompt_eval_count/eval_count on the final chunk.')
    print('3) chunk_count is not token count; eval_count is provider output-token count.')
    print('4) Rough saved input tokens = no-cache input - cache input (iteration matched).')

    if not first_context_seen:
        print('5) RESULT: No reusable context was returned in cache mode for this model/endpoint.')
        print('   Rough savings above are from shorter prompts, not provable provider KV cache reuse.')
    else:
        print('5) RESULT: Reusable context was returned; cache reuse path is available on this model/endpoint.')

    print('=' * 100)
    return 0


if __name__ == '__main__':
    sys.exit(main())
