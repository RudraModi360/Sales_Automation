from instantly.email_generation import client, email_chain_generation,person_data_explorer,save_email_chain_results
from instantly.data_inputs import data_read, external_schema_converter
from groq import Groq
from anthropic import Anthropic
import os
from dotenv import load_dotenv

load_dotenv()

client=Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

client_anthropic = Anthropic(
    api_key=os.getenv("ANTHROPIC_API_KEY")
    )

if __name__ == "__main__":
    df = external_schema_converter(
        data_read(
            file_url="US NY - Financial Services - 10 mn to 500 mn - Copy.csv",
            sheet_name="in",
        )
    )
    
    print("DataFrame loaded and converted successfully.")
    results=email_chain_generation(client=client_anthropic,df=df.iloc[1],person_context=person_data_explorer(client, df.iloc[0]))
    output_file=save_email_chain_results(results)
    print(f"Email chain generation completed. Results saved to {output_file}")
    