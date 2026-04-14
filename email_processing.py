from instantly.email_generation import (
    _build_anthropic_client,
    email_chain_generation,
    person_data_explorer,
    save_email_chain_results,
)
from instantly.data_inputs import data_read, external_schema_converter
from groq import Groq
import os
from dotenv import load_dotenv

load_dotenv()

client=Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

provider_name = (os.getenv("LLM_PROVIDER") or "anthropic").strip().lower()
client_llm = _build_anthropic_client() if provider_name == "anthropic" else None

if __name__ == "__main__":
    df = external_schema_converter(
        data_read(
            file_url="US NY - Financial Services - 10 mn to 500 mn - Copy.csv",
            sheet_name="in",
        )
    )
    
    print("DataFrame loaded and converted successfully.")
    print(f"Using LLM provider: {provider_name}")
    results=email_chain_generation(
        client=client_llm,
        df=df.iloc[1],
        person_context=person_data_explorer(client, df.iloc[1]),
        provider_name=provider_name,
    )
    output_file=save_email_chain_results(results)
    print(f"Email chain generation completed. Results saved to {output_file}")
    