import ollama
import time

model = "qwen3:0.6b"

# Context for cache mode
context_cache = None

# Manual history (for no-cache mode)
history = []


def run_no_cache(user_input):
    global history

    print("\n" + "="*60)
    print(f"NO CACHE ❌ | USER: {user_input}")

    history.append(f"User: {user_input}")

    prompt = "\n".join(history)

    start = time.time()
    first_token_time = None

    stream = ollama.generate(
        model=model,
        prompt=prompt,
        stream=True
    )

    print("ASSISTANT:", end=" ", flush=True)

    full_response = ""
    token_count = 0

    for chunk in stream:
        now = time.time()

        if first_token_time is None:
            first_token_time = now

        token = chunk.get("response", "")
        if token:
            print(token, end="", flush=True)
            full_response += token
            token_count += 1

        prompt_eval = chunk.get("prompt_eval_count")
        eval_count = chunk.get("eval_count")

    total = time.time() - start
    ttft = first_token_time - start if first_token_time else None

    history.append(f"Assistant: {full_response}")

    print("\n--- 📊 TELEMETRY ---")
    print(f"TTFT: {ttft:.3f}s | Total: {total:.3f}s")
    print(f"Tokens Generated: {token_count}")
    if prompt_eval:
        print(f"Prompt Tokens: {prompt_eval}")
    if eval_count:
        print(f"Response Tokens: {eval_count}")


def run_with_cache(user_input):
    global context_cache

    print("\n" + "="*60)
    print(f"WITH CACHE ✅ | USER: {user_input}")

    start = time.time()
    first_token_time = None

    stream = ollama.generate(
        model=model,
        prompt=user_input,        # 🔥 ONLY NEW INPUT
        context=context_cache,    # 🔥 KV reuse
        stream=True
    )

    print("ASSISTANT:", end=" ", flush=True)

    token_count = 0
    latest_context = None

    for chunk in stream:
        now = time.time()

        if first_token_time is None:
            first_token_time = now

        token = chunk.get("response", "")
        if token:
            print(token, end="", flush=True)
            token_count += 1

        if chunk.get("context") is not None:
            latest_context = chunk["context"]

        prompt_eval = chunk.get("prompt_eval_count")
        eval_count = chunk.get("eval_count")

    total = time.time() - start
    ttft = first_token_time - start if first_token_time else None

    if latest_context is not None:
        context_cache = latest_context

    print("\n--- 📊 TELEMETRY ---")
    print(f"TTFT: {ttft:.3f}s | Total: {total:.3f}s")
    print(f"Tokens Generated: {token_count}")
    if prompt_eval:
        print(f"Prompt Tokens: {prompt_eval}")
    if eval_count:
        print(f"Response Tokens: {eval_count}")


# -----------------------
# RUN TEST
# -----------------------

queries = [
    "What's the weather in Ahmedabad?",
    "What about Mumbai?",
    "Compare both cities"
]

print("\n🚫 NO CACHE RUN")
for q in queries:
    run_no_cache(q)

# reset
context_cache = None

print("\n✅ CACHE RUN")
for q in queries:
    run_with_cache(q)